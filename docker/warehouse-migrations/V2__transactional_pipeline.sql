-- 同库 ODS 的事务性变更捕获。change_id 只作唯一标识，不作提交顺序水位线。
CREATE TABLE IF NOT EXISTS dwh.etl_changes (
    change_id BIGSERIAL PRIMARY KEY,
    source_table TEXT NOT NULL CHECK (source_table IN ('ods.orders', 'ods.users')),
    record_key BIGINT,
    operation TEXT NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE')),
    changed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((operation = 'TRUNCATE') = (record_key IS NULL))
);
CREATE TABLE IF NOT EXISTS dwh.etl_pipeline_state (
    pipeline TEXT PRIMARY KEY,
    bootstrapped BOOLEAN NOT NULL DEFAULT false,
    revision BIGINT NOT NULL DEFAULT 0
);
INSERT INTO dwh.etl_pipeline_state (pipeline) VALUES ('sales') ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS dwh.etl_batches (
    batch_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL CHECK (mode IN ('full', 'incremental', 'backfill')),
    arguments JSONB NOT NULL,
    revision BIGINT NOT NULL UNIQUE,
    result JSONB NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    event_delivered_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS etl_batches_pending ON dwh.etl_batches (revision)
    WHERE event_delivered_at IS NULL;
CREATE INDEX IF NOT EXISTS fact_orders_user_id ON dwh.fact_orders (user_id);
CREATE INDEX IF NOT EXISTS fact_orders_order_date ON dwh.fact_orders (order_date);

CREATE OR REPLACE FUNCTION dwh.capture_source_change() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    old_key BIGINT;
    new_key BIGINT;
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        INSERT INTO dwh.etl_changes (source_table, operation)
        VALUES (TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, TG_OP);
        RETURN NULL;
    END IF;
    IF TG_OP <> 'INSERT' THEN
        old_key := (to_jsonb(OLD) ->> TG_ARGV[0])::bigint;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        new_key := (to_jsonb(NEW) ->> TG_ARGV[0])::bigint;
    END IF;
    IF TG_OP = 'UPDATE' AND old_key IS DISTINCT FROM new_key THEN
        INSERT INTO dwh.etl_changes (source_table, record_key, operation)
        VALUES (TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, old_key, 'DELETE');
    END IF;
    INSERT INTO dwh.etl_changes (source_table, record_key, operation)
    VALUES (TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, coalesce(new_key, old_key), TG_OP);
    RETURN NULL;
END;
$$;
CREATE OR REPLACE TRIGGER datamart_orders_changes
AFTER INSERT OR UPDATE OR DELETE ON ods.orders
FOR EACH ROW EXECUTE FUNCTION dwh.capture_source_change('order_id');
CREATE OR REPLACE TRIGGER datamart_users_changes
AFTER INSERT OR UPDATE OR DELETE ON ods.users
FOR EACH ROW EXECUTE FUNCTION dwh.capture_source_change('user_id');
CREATE OR REPLACE TRIGGER datamart_orders_truncate
AFTER TRUNCATE ON ods.orders
FOR EACH STATEMENT EXECUTE FUNCTION dwh.capture_source_change();
CREATE OR REPLACE TRIGGER datamart_users_truncate
AFTER TRUNCATE ON ods.users
FOR EACH STATEMENT EXECUTE FUNCTION dwh.capture_source_change();
