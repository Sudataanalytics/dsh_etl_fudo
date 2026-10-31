# fudo_etl/main.py
import logging
import json
from datetime import datetime, timezone
import uuid
import re
from hashlib import md5
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2

# Tus módulos (importaciones directas/relativas a la raíz del WORKDIR /app en Docker)
# Dentro del contenedor Docker, 'main.py' está en /app, y 'modules' está en /app/modules
from modules.config import load_config
from modules.db_manager import DBManager
from modules.etl_metadata_manager import ETLMetadataManager
from modules.fudo_auth import BranchTokenProvider, FudoAuthenticator, CREDENTIAL_ERRORS
from modules.fudo_api_client import FudoApiClient, EntityForbiddenError
from modules.analytical_layer_mvs import materialized_views_configs, raw_views_configs

# Versión del ETL. Aparece en cada línea de log para identificar qué build corre en GCP.
ETL_VERSION = "V2.0"

# Configuración básica de logging para todo el script principal.
# threadName identifica sucursal/hilo, ya que las entidades se extraen en paralelo.
logging.basicConfig(level=logging.INFO,
                    format=f'##{ETL_VERSION}## %(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ENTITIES_TO_EXTRACT = [
    'customers', 'discounts', 'expenses', 'expense-categories', 'ingredients',
    'items', 'kitchens', 'payments', 'payment-methods', 'product-categories',
    'product-modifiers', 'products', 'roles', 'rooms', 'sales', 'tables', 'users'
]

# Elimina el DROP de las definiciones de MVs para no recrearlas en cada corrida
DROP_MV_PATTERN = re.compile(r"DROP\s+MATERIALIZED\s+VIEW[^;]*;", re.IGNORECASE)
# Índices únicos de cada MV: REFRESH CONCURRENTLY los necesita
UNIQUE_INDEX_PATTERN = re.compile(r"CREATE\s+UNIQUE\s+INDEX[^;]*;", re.IGNORECASE)

# Función auxiliar para parsear fechas de la API
def parse_fudo_date(date_str: str | None) -> datetime | None:
    """
    Parsea una cadena de fecha de Fudo (ISO 8601 con 'Z') a un objeto datetime UTC.
    Maneja None y errores de formato.
    """
    if date_str is None:
        return None
    try:
        # Fudo usa 'Z' para UTC, Python fromisoformat necesita '+00:00'
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except ValueError:
        logger.warning(f"No se pudo parsear la fecha de Fudo: '{date_str}'. Retornando None.")
        return None

def raw_table_for(entity: str) -> str:
    return f"fudo_raw_{entity.replace('-', '_')}"


def prepare_records(page: list[dict], entity: str, id_sucursal: str) -> list[dict]:
    """Convierte una página de la API en registros RAW, deduplicando dentro de la página."""
    extracted_at = datetime.now(timezone.utc)
    prepared = {}
    for record in page:
        fudo_id = record.get('id', str(uuid.uuid4()))
        payload_str = json.dumps(record, sort_keys=True)
        checksum = md5(payload_str.encode('utf-8')).hexdigest()

        attributes = record.get('attributes', {})
        if entity == 'sales':
            last_updated_fudo = parse_fudo_date(attributes.get('closedAt')) or parse_fudo_date(attributes.get('createdAt'))
        else:
            last_updated_fudo = parse_fudo_date(attributes.get('createdAt'))

        # Clave de unicidad: ID + Sucursal + Contenido.
        # En sales la PK es (id_fudo, sucursal): dos versiones en el mismo lote harían fallar el upsert.
        unique_key = fudo_id if entity == 'sales' else (fudo_id, checksum)
        if unique_key not in prepared:
            prepared[unique_key] = {
                'id_fudo': fudo_id,
                'id_sucursal_fuente': id_sucursal,
                'fecha_extraccion_utc': extracted_at,
                'payload_json': payload_str,
                'last_updated_at_fudo': last_updated_fudo,
                'payload_checksum': checksum
            }
    return list(prepared.values())


def extract_and_load_entity(db_manager: DBManager, api_client: FudoApiClient,
                            metadata_manager: ETLMetadataManager, id_sucursal: str,
                            entity: str, run_started_at: datetime) -> int:
    """
    Extrae una entidad de una sucursal página por página y carga cada lote apenas llega.
    La carga conserva el historial de la tabla RAW: solo suma las versiones nuevas (checksum distinto)
    y es idempotente, así que reintentar una entidad que falló a mitad de camino es seguro.
    El checkpoint solo avanza si la entidad terminó completa.
    Devuelve la cantidad de registros procesados.
    """
    raw_table_name = raw_table_for(entity)
    is_full_refresh = entity in api_client.entities_for_full_refresh

    last_ts = metadata_manager.get_last_extraction_timestamp(id_sucursal, entity)
    total_loaded = 0
    for page_number, page in enumerate(api_client.iter_pages(entity, id_sucursal, last_ts), start=1):
        records = prepare_records(page, entity, id_sucursal)
        db_manager.insert_raw_data(raw_table_name, records)
        total_loaded += len(records)
        if page_number % 20 == 0:
            logger.info(f"    '{entity}': {total_loaded} registros cargados hasta la página {page_number}...")

    if total_loaded == 0:
        logger.info(f"    No hay datos nuevos para '{entity}'.")
        return 0

    strategy = "completa" if is_full_refresh else "incremental"
    logger.info(f"    [AUDIT] '{entity}' ({strategy}): {total_loaded} registros procesados en '{raw_table_name}'.")

    # Se guarda el inicio de la corrida (no el final) para no perder registros creados durante la extracción
    metadata_manager.update_last_extraction_timestamp(id_sucursal, entity, run_started_at)
    return total_loaded


def get_worker_counts() -> tuple[int, int]:
    """(sucursales en paralelo, entidades en paralelo por sucursal). Requests simultáneos a Fudo = producto."""
    branch_workers = max(1, int(os.getenv("ETL_BRANCH_WORKERS", "3")))
    entity_workers = max(1, int(os.getenv("ETL_ENTITY_WORKERS", "2")))
    return branch_workers, entity_workers


def authenticate_branches(authenticator: FudoAuthenticator,
                          branches_config: list[tuple]) -> tuple[list[tuple], list[str]]:
    """
    Obtiene el token de cada sucursal antes de extraer.
    Devuelve (sucursales disponibles como (branch_data, token_provider), ids de las omitidas).
    Una sucursal sin credenciales válidas no frena al resto ni cuenta como error del ETL:
    se informa en el log con el prefijo [CREDENCIALES] para poder filtrarla o alertar sobre ella.
    """
    available, skipped = [], []
    for branch_data in branches_config:
        id_sucursal, _, branch_name, api_key_secret_name, api_secret_secret_name = branch_data
        try:
            token_provider = BranchTokenProvider(authenticator, id_sucursal, api_key_secret_name, api_secret_secret_name)
            available.append((branch_data, token_provider))
        except CREDENTIAL_ERRORS as e:
            logger.warning(f"[CREDENCIALES] Sucursal '{branch_name}' ({id_sucursal}) omitida: {e}")
            skipped.append(id_sucursal)
        except Exception as e:
            # Falla inesperada (red, base, respuesta rara de Fudo): se omite igual, pero se deja el detalle
            logger.error(f"[CREDENCIALES] Sucursal '{branch_name}' ({id_sucursal}) omitida: {e}", exc_info=True)
            skipped.append(id_sucursal)

    logger.info(f"[CREDENCIALES] {len(available)}/{len(branches_config)} sucursales con credenciales válidas.")
    if skipped:
        logger.warning(f"[CREDENCIALES] Sucursales omitidas en esta corrida: {', '.join(skipped)}")
    return available, skipped


def extract_branch(db_manager: DBManager, metadata_manager: ETLMetadataManager, api_base_url: str,
                   branch_data: tuple, token_provider: BranchTokenProvider,
                   run_started_at: datetime, entity_workers: int) -> list[str]:
    """
    Extrae todas las entidades de una sucursal ya autenticada, varias a la vez.
    Cada entidad usa su propio FudoApiClient (requests.Session no se comparte entre hilos),
    todas con el mismo token de la sucursal. Nunca lanza: devuelve la lista de fallos.
    Una entidad sin permiso en Fudo (403) se omite con aviso [PERMISOS] y no cuenta como fallo.
    """
    id_sucursal, _, branch_name, _, _ = branch_data
    logger.info(f"--- Procesando Sucursal: '{branch_name}' ({id_sucursal}) ---")

    def run_entity(entity: str):
        api_client = FudoApiClient(api_base_url)
        try:
            api_client.set_auth_token(token_provider.token)
            api_client.token_refresher = lambda: token_provider.refresh(api_client.auth_token)
            extract_and_load_entity(db_manager, api_client, metadata_manager, id_sucursal, entity, run_started_at)
        finally:
            api_client.close()

    failures, forbidden = [], []
    with ThreadPoolExecutor(max_workers=entity_workers, thread_name_prefix=f"{id_sucursal}") as executor:
        futures = {executor.submit(run_entity, entity): entity for entity in ENTITIES_TO_EXTRACT}
        for future in as_completed(futures):
            entity = futures[future]
            try:
                future.result()
            except EntityForbiddenError as e:
                logger.warning(f"[PERMISOS] Entidad '{entity}' (sucursal '{branch_name}') omitida: {e}")
                forbidden.append(entity)
            except Exception as e:
                logger.error(f"  Error en entidad '{entity}' (sucursal '{branch_name}'): {e}", exc_info=True)
                failures.append(f"{id_sucursal}/{entity}")

    if forbidden:
        logger.warning(f"[PERMISOS] Sucursal '{branch_name}' ({id_sucursal}) sin permiso en Fudo para: {', '.join(forbidden)}")
    logger.info(f"--- Sucursal '{branch_name}' finalizada ({len(failures)} fallos, {len(forbidden)} sin permiso) ---")
    return failures


# --- FUNCIÓN PARA LA FASE DE TRANSFORMACIÓN Y CARGA AL DER ---
def refresh_analytics_materialized_views(db_manager: DBManager,
                                          materialized_views_configs: list[tuple[str, str]],
                                          raw_views_configs: list[tuple[str, str]]) -> list[str]:
    """
    Crea las MVs solo si no existen (o si REBUILD_MVS=true) y luego las refresca CONCURRENTLY,
    lo que permite que Power BI siga leyendo durante el refresco.
    Devuelve la lista de objetos que fallaron.
    """
    logger.info("==================================================")
    logger.info("  Iniciando fase de Transformación (Creación/Refresco de MVs y Vistas RAW)")
    logger.info("==================================================")

    rebuild_mvs = os.getenv("REBUILD_MVS", "false").lower() == "true"
    failures = []

    for view_name, create_sql in raw_views_configs:
        logger.info(f"  Procesando Vista RAW Desnormalizada: '{view_name}'...")
        try:
            db_manager.execute_query(create_sql)
        except Exception as e:
            logger.error(f"  ERROR al procesar la Vista RAW '{view_name}': {e}")
            failures.append(view_name)

    for mv_name, create_sql in materialized_views_configs:
        try:
            exists = db_manager.fetch_one(
                "SELECT 1 FROM pg_matviews WHERE schemaname = 'public' AND matviewname = %s;", (mv_name,)
            )
            if rebuild_mvs:
                logger.info(f"  Recreando MV '{mv_name}' (REBUILD_MVS=true)...")
                db_manager.execute_query(create_sql)
            elif not exists:
                logger.info(f"  Creando MV '{mv_name}' (no existía)...")
                db_manager.execute_query(DROP_MV_PATTERN.sub("", create_sql))
            else:
                # Una MV creada antes de que su definición tuviera índice único no lo tiene todavía
                for index_sql in UNIQUE_INDEX_PATTERN.findall(create_sql):
                    db_manager.execute_query(index_sql)
                logger.info(f"  Refrescando MV '{mv_name}' CONCURRENTLY...")
                try:
                    db_manager.execute_query(f"REFRESH MATERIALIZED VIEW CONCURRENTLY public.{mv_name};")
                except (psycopg2.errors.FeatureNotSupported, psycopg2.errors.ObjectNotInPrerequisiteState) as e:
                    # CONCURRENTLY requiere un índice único y que la MV esté poblada
                    logger.warning(f"  No se pudo refrescar '{mv_name}' CONCURRENTLY ({e}). Usando REFRESH normal.")
                    db_manager.execute_query(f"REFRESH MATERIALIZED VIEW public.{mv_name};")
            logger.info(f"  MV '{mv_name}' OK.")
        except Exception as e:
            logger.error(f"  ERROR al procesar la Vista Materializada '{mv_name}': {e}")
            failures.append(mv_name)

    logger.info("==================================================")
    logger.info("  Fase de Transformación (Creación/Refresco de MVs y Vistas RAW) FINALIZADA.")
    logger.info("==================================================")
    return failures


def run_fudo_raw_etl(db_manager: DBManager) -> list[str]:
    """
    Ejecuta la extracción y carga de todas las sucursales activas y luego la transformación.
    Primero valida las credenciales: las sucursales sin credenciales válidas se omiten y solo
    quedan en el log (no cuentan como error, salvo que no quede ninguna disponible).
    Un fallo en una entidad no frena el resto (su checkpoint no avanza),
    pero se registra y se devuelve para que el proceso termine con error.
    """
    logger.info("==================================================")
    logger.info("  Iniciando proceso ETL RAW de Fudo - EXTRACT & LOAD")
    logger.info("==================================================")

    run_started_at = datetime.now(timezone.utc)
    config = load_config()
    project_id = config.get("gcp_project_id")
    metadata_manager = ETLMetadataManager(db_manager)
    authenticator = FudoAuthenticator(db_manager, config['fudo_auth_endpoint'], project_id)
    api_base_url = config['fudo_api_base_url']
    failures = []

    logger.info("Obteniendo lista de sucursales activas de la base de datos...")
    branches_config = db_manager.fetch_all(
        "SELECT id_sucursal, fudo_branch_identifier, sucursal_name, "
        "secret_manager_apikey_name, secret_manager_apisecret_name "
        "FROM public.config_fudo_branches WHERE is_active = TRUE"
    )
    if not branches_config:
        logger.warning("No se encontraron sucursales activas para procesar en config_fudo_branches.")
        return ["sin_sucursales_activas"]

    available_branches, _ = authenticate_branches(authenticator, branches_config)
    if not available_branches:
        logger.error("[CREDENCIALES] Ninguna sucursal tiene credenciales válidas. No se extrae nada.")
        return ["sin_sucursales_con_credenciales"]

    branch_workers, entity_workers = get_worker_counts()
    logger.info(f"Extrayendo {len(available_branches)} sucursales "
                f"({branch_workers} sucursales x {entity_workers} entidades en paralelo).")

    with ThreadPoolExecutor(max_workers=branch_workers, thread_name_prefix="sucursal") as executor:
        futures = [
            executor.submit(extract_branch, db_manager, metadata_manager, api_base_url,
                            branch_data, token_provider, run_started_at, entity_workers)
            for branch_data, token_provider in available_branches
        ]
        for future in as_completed(futures):
            failures.extend(future.result())

    # Cada lote se carga en su propia transacción y la carga es idempotente, así que las MVs
    # se refrescan aunque alguna entidad haya fallado (se reintenta completa en la próxima corrida).
    failures.extend(refresh_analytics_materialized_views(db_manager, materialized_views_configs, raw_views_configs))
    return failures


# --- FUNCIÓN PARA DESPLEGAR LA ESTRUCTURA INICIAL DE FUDO EN LA DB ---
def deploy_fudo_database_structure(db_manager: DBManager, ddl_script_path: str):
    logger.info("==================================================")
    logger.info(f"  Iniciando despliegue de estructura Fudo desde '{ddl_script_path}'")
    logger.info("==================================================")
    try:
        # Leer el script DDL completo
        current_dir = os.path.dirname(os.path.abspath(__file__))
        absolute_ddl_path = os.path.join(current_dir, ddl_script_path)

        with open(absolute_ddl_path, 'r', encoding='utf-8') as f:
            sql_script_content = f.read()

        db_manager.execute_sql_script(sql_script_content)
        logger.info("Estructura de la base de datos Fudo desplegada/actualizada exitosamente.")
    except Exception as e:
        logger.critical(f"ERROR FATAL al desplegar la estructura de la base de datos Fudo: {e}", exc_info=True)
        raise
    logger.info("==================================================")
    logger.info("  Despliegue de estructura Fudo FINALIZADO.")
    logger.info("==================================================")


def main() -> int:
    config = load_config()
    db_conn_string = config['db_connection_string']  # La cadena de conexión del ETL (apunta a la DB 'ginesta')

    db_for_all_phases = None
    exit_code = 0
    try:
        # Una conexión por hilo de extracción + una para el hilo principal (metadata, MVs)
        branch_workers, entity_workers = get_worker_counts()
        pool_size = int(os.getenv("DB_POOL_SIZE", str(branch_workers * entity_workers + 1)))
        db_for_all_phases = DBManager(db_conn_string, pool_size=pool_size)

        # Paso 1: Ejecutar el script DDL maestro para crear/actualizar la estructura (tablas, sucursales, índices).
        deploy_fudo_database_structure(db_for_all_phases, 'sql/deploy_fudo_structure.sql')

        # Paso 2: Ejecutar el ETL RAW completo y la fase de refresco de MVs
        logger.info("\nIniciando fase de EJECUCIÓN REGULAR del ETL (EXTRACCIÓN y TRANSFORMACIÓN)...")
        failures = run_fudo_raw_etl(db_for_all_phases)

        if failures:
            logger.error(f"ETL finalizado CON ERRORES ({len(failures)}): {', '.join(failures)}")
            exit_code = 1
        else:
            logger.info("\n¡Proceso ETL de Fudo (Extracción y Transformación) finalizado con éxito!")

    except Exception as e:
        logger.critical(f"ERROR FATAL en el proceso ETL principal: {e}", exc_info=True)
        exit_code = 1
    finally:
        if db_for_all_phases:
            db_for_all_phases.close()
        logger.info("==================================================")
        logger.info(f"  FINALIZACIÓN COMPLETA DEL SCRIPT ETL (exit code {exit_code}).")
        logger.info("==================================================")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
