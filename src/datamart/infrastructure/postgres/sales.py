"""Sales SQL repository. Every operation uses the caller's transaction cursor."""

from datamart.errors import PipelineQualityError


class PostgresSalesRepository:
    @staticmethod
    def assert_capture_enabled(cur):
        cur.execute(
            "SELECT count(*) FROM pg_trigger "
            "WHERE tgrelid IN ('ods.orders'::regclass, 'ods.users'::regclass) "
            "AND tgname IN ('datamart_orders_changes', 'datamart_users_changes', "
            "'datamart_orders_truncate', 'datamart_users_truncate') AND tgenabled IN ('O', 'A')"
        )
        if cur.fetchone()[0] != 4:
            raise PipelineQualityError("Source change capture is missing or disabled; refuse publication")

    @staticmethod
    def capture_keys(cur, full):
        if full:
            cur.execute(
                "CREATE TEMP TABLE batch_user_keys ON COMMIT DROP AS "
                "SELECT user_id FROM ods.users UNION SELECT user_id FROM dwh.dim_users"
            )
            cur.execute(
                "CREATE TEMP TABLE batch_order_keys ON COMMIT DROP AS "
                "SELECT order_id FROM ods.orders UNION SELECT order_id FROM dwh.fact_orders"
            )
        else:
            cur.execute(
                "CREATE TEMP TABLE batch_user_keys ON COMMIT DROP AS "
                "SELECT DISTINCT record_key AS user_id FROM batch_changes WHERE source_table = 'ods.users'"
            )
            cur.execute(
                "CREATE TEMP TABLE batch_order_keys ON COMMIT DROP AS "
                "SELECT record_key AS order_id FROM batch_changes WHERE source_table = 'ods.orders' "
                "UNION SELECT o.order_id FROM ods.orders o JOIN batch_user_keys u USING (user_id) "
                "UNION SELECT f.order_id FROM dwh.fact_orders f JOIN batch_user_keys u USING (user_id)"
            )
        cur.execute("CREATE UNIQUE INDEX ON batch_order_keys (order_id)")
        cur.execute("CREATE UNIQUE INDEX ON batch_user_keys (user_id)")
        cur.execute(
            "INSERT INTO batch_dates SELECT f.order_date FROM dwh.fact_orders f "
            "JOIN batch_order_keys k USING (order_id) UNION SELECT o.order_date FROM ods.orders o "
            "JOIN batch_order_keys k USING (order_id) ON CONFLICT DO NOTHING"
        )
        if full:
            cur.execute(
                "INSERT INTO batch_dates SELECT stat_date FROM dm.daily_city_sales "
                "UNION SELECT stat_date FROM dm.daily_summary_report ON CONFLICT DO NOTHING"
            )

    @staticmethod
    def merge(cur):
        cur.execute(
            "SELECT count(*) FROM ods.orders o JOIN batch_order_keys k USING (order_id) "
            "LEFT JOIN ods.users u USING (user_id) WHERE u.user_id IS NULL"
        )
        if cur.fetchone()[0]:
            raise PipelineQualityError("Source orders reference missing users")
        cur.execute(
            "INSERT INTO dwh.dim_users AS d (user_id, user_name, city, updated_at) "
            "SELECT u.user_id, u.user_name, u.city, u.updated_at FROM ods.users u "
            "JOIN batch_user_keys k USING (user_id) ON CONFLICT (user_id) DO UPDATE SET "
            "user_name = EXCLUDED.user_name, city = EXCLUDED.city, updated_at = EXCLUDED.updated_at "
            "WHERE (d.user_name, d.city, d.updated_at) IS DISTINCT FROM "
            "(EXCLUDED.user_name, EXCLUDED.city, EXCLUDED.updated_at)"
        )
        cur.execute(
            "INSERT INTO dwh.fact_orders AS f "
            "(order_id, user_id, user_name, city, amount, status, order_date, etl_date) "
            "SELECT o.order_id, o.user_id, u.user_name, u.city, o.amount, o.status, "
            "o.order_date, CURRENT_DATE "
            "FROM ods.orders o JOIN batch_order_keys k USING (order_id) JOIN ods.users u USING (user_id) "
            "ON CONFLICT (order_id) DO UPDATE SET user_id = EXCLUDED.user_id, "
            "user_name = EXCLUDED.user_name, "
            "city = EXCLUDED.city, amount = EXCLUDED.amount, status = EXCLUDED.status, "
            "order_date = EXCLUDED.order_date, etl_date = EXCLUDED.etl_date "
            "WHERE (f.user_id, f.user_name, f.city, f.amount, f.status, f.order_date) IS DISTINCT FROM "
            "(EXCLUDED.user_id, EXCLUDED.user_name, EXCLUDED.city, EXCLUDED.amount, EXCLUDED.status, "
            "EXCLUDED.order_date)"
        )
        changed = cur.rowcount
        cur.execute(
            "DELETE FROM dwh.fact_orders f USING batch_order_keys k WHERE f.order_id = k.order_id "
            "AND NOT EXISTS (SELECT 1 FROM ods.orders o WHERE o.order_id = f.order_id)"
        )
        deleted = cur.rowcount
        cur.execute(
            "DELETE FROM dwh.dim_users d USING batch_user_keys k WHERE d.user_id = k.user_id "
            "AND NOT EXISTS (SELECT 1 FROM ods.users u WHERE u.user_id = d.user_id)"
        )
        return changed, deleted

    @staticmethod
    def refresh(cur, checkpoint, conn):
        cur.execute("DELETE FROM dm.daily_city_sales d USING batch_dates b WHERE d.stat_date = b.stat_date")
        checkpoint("after_mart_delete", conn) if checkpoint else None
        cur.execute(
            "INSERT INTO dm.daily_city_sales (stat_date, city, order_cnt, total_amount) "
            "SELECT f.order_date, f.city, count(*), sum(f.amount) FROM dwh.fact_orders f "
            "JOIN batch_dates b ON b.stat_date = f.order_date WHERE f.status = 'paid' "
            "GROUP BY f.order_date, f.city"
        )
        cur.execute(
            "DELETE FROM dm.daily_summary_report d USING batch_dates b WHERE d.stat_date = b.stat_date"
        )
        cur.execute(
            "INSERT INTO dm.daily_summary_report (stat_date, city_cnt, order_cnt, total_amount) "
            "SELECT d.stat_date, count(*), sum(d.order_cnt), sum(d.total_amount) FROM dm.daily_city_sales d "
            "JOIN batch_dates b USING (stat_date) GROUP BY d.stat_date"
        )

    @staticmethod
    def assert_source_matches(cur):
        cur.execute(
            "WITH expected AS (SELECT o.order_id, o.user_id, u.user_name, u.city, "
            "o.amount, o.status, o.order_date "
            "FROM ods.orders o JOIN batch_order_keys k USING (order_id) JOIN ods.users u USING (user_id)), "
            "actual AS (SELECT f.order_id, f.user_id, f.user_name, f.city, f.amount, f.status, f.order_date "
            "FROM dwh.fact_orders f JOIN batch_order_keys k USING (order_id)) "
            "SELECT EXISTS ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) "
            "UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected))"
        )
        if cur.fetchone()[0]:
            raise PipelineQualityError("Source/fact field reconciliation failed")

    @staticmethod
    def assert_aggregates(cur):
        cur.execute(
            "WITH expected AS (SELECT order_date, city, count(*)::int, sum(amount) FROM dwh.fact_orders "
            "WHERE status = 'paid' GROUP BY order_date, city), "
            "actual AS (SELECT stat_date, city, order_cnt, total_amount FROM dm.daily_city_sales) "
            "SELECT EXISTS ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) "
            "UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected))"
        )
        if cur.fetchone()[0]:
            raise PipelineQualityError("Fact/mart date, city, count or amount reconciliation failed")
        cur.execute(
            "WITH expected AS (SELECT stat_date, count(*)::int, sum(order_cnt)::int, sum(total_amount) "
            "FROM dm.daily_city_sales GROUP BY stat_date), "
            "actual AS (SELECT stat_date, city_cnt, order_cnt, total_amount FROM dm.daily_summary_report) "
            "SELECT EXISTS ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) "
            "UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected))"
        )
        if cur.fetchone()[0]:
            raise PipelineQualityError("Mart/report reconciliation failed")
