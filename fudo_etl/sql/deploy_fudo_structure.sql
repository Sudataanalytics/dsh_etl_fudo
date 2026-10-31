-- ######################################################################
-- #            SCRIPT MAESTRO DE DESPLIEGUE PARA DONWEB                #
-- #            ESTRUCTURA DE DATOS ETL FUDO (RAW y ANALÍTICA)          #
-- #            DB: ginesta                                             #
-- ######################################################################

-- NOTA IMPORTANTE:
-- Este script ASUME que la base de datos 'ginesta' YA HA SIDO CREADA.
-- Este script DEBE ejecutarse CONECTADO a la base de datos 'ginesta'
-- utilizando el usuario 'sudata_owner'.

-- ----------------------------------------------------------------------
-- PERMISOS INICIALES (Para el usuario 'sudata_owner' en el esquema 'public')
-- Esto garantiza que sudata_owner tenga control total sobre los objetos.
-- ----------------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO sudata_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO sudata_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO sudata_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO sudata_owner;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sudata_owner;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO sudata_owner;

-- ----------------------------------------------------------------------
-- 1. TABLAS DE METADATOS Y CONFIGURACIÓN DEL ETL
-- ----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.etl_fudo_tokens (
    id_sucursal VARCHAR(255) PRIMARY KEY,
    access_token TEXT NOT NULL,
    token_expiration_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    last_updated_utc TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.etl_fudo_extraction_status (
    id_sucursal VARCHAR(255) NOT NULL,
    entity_name VARCHAR(100) NOT NULL,
    last_successful_extraction_utc TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (id_sucursal, entity_name)
);

CREATE TABLE IF NOT EXISTS public.config_fudo_branches (
    id_sucursal VARCHAR(255) PRIMARY KEY,
    fudo_branch_identifier VARCHAR(255),
    sucursal_name VARCHAR(255),
    secret_manager_apikey_name VARCHAR(255) NOT NULL,
    secret_manager_apisecret_name VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
DELETE FROM public.config_fudo_branches WHERE id_sucursal = 'punto_criollo';

-- Insertar las sucursales Fudo iniciales
INSERT INTO public.config_fudo_branches (id_sucursal, fudo_branch_identifier, sucursal_name, secret_manager_apikey_name, secret_manager_apisecret_name)
VALUES ('chale', 'MUAxNTk0MzM=', 'Chale', 'FUDO_CHALE_APIKEY', 'FUDO_CHALE_APISECRET')
ON CONFLICT (id_sucursal) DO UPDATE SET
    fudo_branch_identifier = EXCLUDED.fudo_branch_identifier, sucursal_name = EXCLUDED.sucursal_name,
    secret_manager_apikey_name = EXCLUDED.secret_manager_apikey_name, secret_manager_apisecret_name = EXCLUDED.secret_manager_apisecret_name,
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO public.config_fudo_branches (id_sucursal, fudo_branch_identifier, sucursal_name, secret_manager_apikey_name, secret_manager_apisecret_name)
VALUES ('quito_bar', 'M0AyMjA0NDQ=', 'Quito Bar', 'FUDO_QUITO_BAR_APIKEY', 'FUDO_QUITO_BAR_APISECRET')
ON CONFLICT (id_sucursal) DO UPDATE SET
    fudo_branch_identifier = EXCLUDED.fudo_branch_identifier, sucursal_name = EXCLUDED.sucursal_name,
    secret_manager_apikey_name = EXCLUDED.secret_manager_apikey_name, secret_manager_apisecret_name = EXCLUDED.secret_manager_apisecret_name,
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO public.config_fudo_branches (id_sucursal, fudo_branch_identifier, sucursal_name, secret_manager_apikey_name, secret_manager_apisecret_name)
VALUES ('nebraska_sas', 'MjBAMTU1NjMy', 'Nebraska SAS', 'FUDO_NEBRASKA_SAS_APIKEY', 'FUDO_NEBRASKA_SAS_APISECRET')
ON CONFLICT (id_sucursal) DO UPDATE SET
    fudo_branch_identifier = EXCLUDED.fudo_branch_identifier, sucursal_name = EXCLUDED.sucursal_name,
    secret_manager_apikey_name = EXCLUDED.secret_manager_apikey_name, secret_manager_apisecret_name = EXCLUDED.secret_manager_apisecret_name,
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO public.config_fudo_branches (id_sucursal, fudo_branch_identifier, sucursal_name, secret_manager_apikey_name, secret_manager_apisecret_name)
VALUES ('punto_criollo-peron', 'MTVANDg2OTc=', 'Punto Criollo Peron', 'FUDO_PUNTO_CRIOLLO_PERON_APIKEY', 'FUDO_PUNTO_CRIOLLO_PERON_APISECRET')
ON CONFLICT (id_sucursal) DO UPDATE SET
    fudo_branch_identifier = EXCLUDED.fudo_branch_identifier, sucursal_name = EXCLUDED.sucursal_name,
    secret_manager_apikey_name = EXCLUDED.secret_manager_apikey_name, secret_manager_apisecret_name = EXCLUDED.secret_manager_apisecret_name,
    updated_at = CURRENT_TIMESTAMP;

-- Añadir la nueva sucursal 'punto_criollo-guemes'
INSERT INTO public.config_fudo_branches (id_sucursal, fudo_branch_identifier, sucursal_name, secret_manager_apikey_name, secret_manager_apisecret_name)
VALUES ('punto_criollo-guemes', 'MTZAMjc1MTM5', 'Punto Criollo Guemes', 'FUDO_PUNTO_CRIOLLO_GUEMES_APIKEY', 'FUDO_PUNTO_CRIOLLO_GUEMES_APISECRET')
ON CONFLICT (id_sucursal) DO UPDATE SET
    fudo_branch_identifier = EXCLUDED.fudo_branch_identifier, sucursal_name = EXCLUDED.sucursal_name,
    secret_manager_apikey_name = EXCLUDED.secret_manager_apikey_name, secret_manager_apisecret_name = EXCLUDED.secret_manager_apisecret_name,
    updated_at = CURRENT_TIMESTAMP;

-- Añadir la nueva sucursal 'nani_panaderia'
INSERT INTO public.config_fudo_branches (id_sucursal, fudo_branch_identifier, sucursal_name, secret_manager_apikey_name, secret_manager_apisecret_name)
VALUES ('nani_pasteleria', 'MThAMjgyODQz', 'Nani Pastelería', 'FUDO_NANI_PASTELERIA_APIKEY', 'FUDO_NANI_PASTELERIA_APISECRET')
ON CONFLICT (id_sucursal) DO UPDATE SET
    fudo_branch_identifier = EXCLUDED.fudo_branch_identifier, sucursal_name = EXCLUDED.sucursal_name,
    secret_manager_apikey_name = EXCLUDED.secret_manager_apikey_name, secret_manager_apisecret_name = EXCLUDED.secret_manager_apisecret_name,
    updated_at = CURRENT_TIMESTAMP;

-- Añadir la nueva sucursal 'nani_panaderia'
INSERT INTO public.config_fudo_branches (id_sucursal, fudo_branch_identifier, sucursal_name, secret_manager_apikey_name, secret_manager_apisecret_name)
VALUES ('policastro', 'MTNAMzU2MjQ1', 'Policastro', 'FUDO_POLICASTRO_APIKEY', 'FUDO_POLICASTRO_APISECRET')
ON CONFLICT (id_sucursal) DO UPDATE SET
    fudo_branch_identifier = EXCLUDED.fudo_branch_identifier, 
    sucursal_name = EXCLUDED.sucursal_name,
    secret_manager_apikey_name = EXCLUDED.secret_manager_apikey_name, 
    secret_manager_apisecret_name = EXCLUDED.secret_manager_apisecret_name,
    updated_at = CURRENT_TIMESTAMP;

-- ----------------------------------------------------------------------
-- 2. TABLAS PARA LA CAPA DE DATOS CRUDOS (RAW LAYER)
-- ----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.fudo_raw_customers (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL, 
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_customers_id_sucursal_fecha ON public.fudo_raw_customers (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_discounts (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_discounts_id_sucursal_fecha ON public.fudo_raw_discounts (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_expenses (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_expenses_id_sucursal_fecha ON public.fudo_raw_expenses (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_expense_categories (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_expense_categories_id_sucursal_fecha ON public.fudo_raw_expense_categories (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_ingredients (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_ingredients_id_sucursal_fecha ON public.fudo_raw_ingredients (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_items (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_items_id_sucursal_fecha ON public.fudo_raw_items (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_kitchens (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_kitchens_id_sucursal_fecha ON public.fudo_raw_kitchens (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_payments (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_payments_id_sucursal_fecha ON public.fudo_raw_payments (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_payment_methods (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_payment_methods_id_sucursal_fecha ON public.fudo_raw_payment_methods (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_product_categories (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_product_categories_id_sucursal_fecha ON public.fudo_raw_product_categories (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_product_modifiers (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_product_modifiers_id_sucursal_fecha ON public.fudo_raw_product_modifiers (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_products ( 
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_products_id_sucursal_fecha ON public.fudo_raw_products (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_roles (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_roles_id_sucursal_fecha ON public.fudo_raw_roles (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_rooms (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_rooms_id_sucursal_fecha ON public.fudo_raw_rooms (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_sales (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_sales_id_sucursal_fecha ON public.fudo_raw_sales (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_tables (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_tables_id_sucursal_fecha ON public.fudo_raw_tables (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_users (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_users_id_sucursal_fecha ON public.fudo_raw_users (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

-- ----------------------------------------------------------------------
-- 3. TABLAS PARA LA CAPA ANALÍTICA (DER)
-- Las vistas materializadas (mv_*) y las vistas RAW (fudo_view_raw_*) se definen en
-- modules/analytical_layer_mvs.py: main.py las crea si no existen y las refresca CONCURRENTLY.
-- ----------------------------------------------------------------------

-- Sucursales (DER)
CREATE TABLE IF NOT EXISTS public.Sucursales (
  id_sucursal VARCHAR(255) PRIMARY KEY,
  sucursal VARCHAR(255) NOT NULL
);

-- Rubros (DER)
CREATE TABLE IF NOT EXISTS public.Rubros (
  id_rubro INTEGER PRIMARY KEY,
  rubro_name VARCHAR(255) NOT NULL
);

-- Medio_pago (DER)
CREATE TABLE IF NOT EXISTS public.Medio_pago (
  id_payment INTEGER PRIMARY KEY,
  payment_method VARCHAR(255) NOT NULL
);

-- Productos (DER)
CREATE TABLE IF NOT EXISTS public.Productos (
  id_product_fudo INTEGER,
  id_sucursal VARCHAR(255),
  product_key TEXT PRIMARY KEY, -- Nueva PK
  product_name VARCHAR(255) NOT NULL,
  id_rubro_fudo INTEGER,
  rubro_key_fk TEXT -- Nueva FK
);

-- Sales_order (DER)
CREATE TABLE IF NOT EXISTS public.Sales_order (
  id_order INTEGER PRIMARY KEY,
  amount_tax FLOAT,
  amount_total FLOAT NOT NULL,
  date_order TIMESTAMP NOT NULL
);
-- Añadir las nuevas columnas si no existen (idempotente)
ALTER TABLE public.Sales_order ADD COLUMN IF NOT EXISTS sale_type VARCHAR(50);
ALTER TABLE public.Sales_order ADD COLUMN IF NOT EXISTS table_id TEXT;
ALTER TABLE public.Sales_order ADD COLUMN IF NOT EXISTS waiter_id TEXT;

-- Pagos (DER)
CREATE TABLE IF NOT EXISTS public.Pagos (
  id INTEGER PRIMARY KEY,
  pos_order_id INTEGER NOT NULL,
  id_payment INTEGER NOT NULL,
  amount FLOAT NOT NULL,
  payment_date TIMESTAMP NOT NULL,
  id_sucursal VARCHAR(255) NOT NULL
);

-- Sales_order_line (DER)
CREATE TABLE IF NOT EXISTS public.Sales_order_line (
  id_order_line INTEGER PRIMARY KEY,
  id_order_fudo INTEGER NOT NULL,
  date_order_time TIMESTAMP WITH TIME ZONE NOT NULL,
  date_order DATE NOT NULL,
  id_product_fudo INTEGER NOT NULL,
  qty_from_api FLOAT NOT NULL,
  price_from_api FLOAT NOT NULL,
  price_unit FLOAT NOT NULL,
  qty INTEGER NOT NULL,
  amount_total FLOAT NOT NULL,
  id_sucursal VARCHAR(255) NOT NULL,
  order_line_key TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS public.Expenses (
  id_expense TEXT PRIMARY KEY,
  id_sucursal VARCHAR(255) NOT NULL,
  amount FLOAT NOT NULL,
  description TEXT,
  expense_date TIMESTAMP NOT NULL,
  status VARCHAR(50),
  due_date TIMESTAMP,
  canceled BOOLEAN,
  created_at TIMESTAMP,
  payment_date TIMESTAMP,
  receipt_number TEXT,
  use_in_cash_count BOOLEAN,
  user_id TEXT,
  provider_id TEXT,
  receipt_type_id TEXT,
  cash_register_id TEXT,
  payment_method_id TEXT,
  expense_category_id TEXT, -- Mantener el id_expense_category de Fudo
  expense_category_key TEXT NOT NULL -- <--- ¡AÑADIR LA FK A LA CLAVE SINTÉTICA!
);
-- Product_categories (Tabla Lógica del DER - para categorización de productos)
CREATE TABLE IF NOT EXISTS public.Product_categories (
  id_product_category INTEGER,
  product_category_name VARCHAR(255) NOT NULL,
  "position" INTEGER,
  preparation_time INTEGER,
  enable_online_menu BOOLEAN,
  kitchen_id TEXT,
  parent_category_id TEXT,
  id_sucursal VARCHAR(255) NOT NULL,
  product_category_key TEXT PRIMARY KEY
);
-- Product_prices_by_branch (Precio y Stock de Producto por Sucursal)
CREATE TABLE IF NOT EXISTS public.Product_prices_by_branch (
  id_product_fudo TEXT NOT NULL,         -- ID original de Fudo (FK a mv_productos)
  id_sucursal VARCHAR(255) NOT NULL,     -- ID de Sucursal (FK a mv_sucursales)
  product_name VARCHAR(255) NOT NULL,    -- Nombre del producto en esta sucursal (para referencia)
  price FLOAT,                           -- Precio base del producto en esa sucursal
  stock FLOAT,                           -- Stock del producto en esa sucursal
  is_active_in_branch BOOLEAN,           -- Si el producto está activo en esa sucursal
  product_branch_key TEXT PRIMARY KEY    -- Clave sintética para esta granularidad
);

-- ----------------------------------------------------------------------
-- 4. ÍNDICES POR SUCURSAL (RAW)
-- ----------------------------------------------------------------------
-- Las consultas por sucursal (WHERE id_sucursal_fuente = ...) no pueden usar la PK, que empieza
-- por id_fudo: sin este índice se recorre la tabla entera.

DO $$
DECLARE
    t RECORD;
BEGIN
    FOR t IN
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename LIKE 'fudo\_raw\_%'
          AND NOT EXISTS (
              SELECT 1 FROM pg_indexes i
              WHERE i.schemaname = 'public' AND i.tablename = pg_tables.tablename
                AND i.indexdef LIKE '%(id_sucursal_fuente)'
          )
    LOOP
        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS %I ON public.%I (id_sucursal_fuente);',
            'idx_' || t.tablename || '_sucursal', t.tablename
        );
    END LOOP;
END $$;
