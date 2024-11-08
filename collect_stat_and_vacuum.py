import psycopg2
from psycopg2 import sql
import secr
from datetime import datetime, timedelta, timezone

# Database connection parameters
host = "10.122.3.134"
port = "5432"
dbname = "greenplum-dwh"
user = "gpadmin"
password = secr.pss()

# Time thresholds
analyze_threshold = timedelta(days=1)
vacuum_threshold = timedelta(days=5)

def collect_stat_and_vacuum_for_missing_tables():
    try:
        cur = None
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        conn.autocommit = True  # Set autocommit to True to run VACUUM
        cur = conn.cursor()

        cur.execute("""
         WITH tab_stats AS (
            SELECT 
                schemaname,
                relname AS tablename,
                n_live_tup AS live_rows,          -- Количество "живых" строк
                n_dead_tup AS dead_rows,          -- Количество "мертвых" строк (подлежащих очистке)
                last_vacuum,                      -- Дата последнего VACUUM
                last_analyze,                     -- Дата последнего analyze
                seq_scan                          -- Количество сканирований таблицы
            FROM 
                pg_stat_user_tables
        ),
        frequent_upd AS (
            -- Ищем таблицы, которые часто обновляются или удаляются
            SELECT 
                schemaname, 
                tablename
            FROM 
                tab_stats
            WHERE 
                dead_rows > 0                   -- Если есть мертвые строки, нужно VACUUM
                OR last_analyze IS NULL           -- Если никогда не проводился ANALYZE
                OR last_vacuum IS NULL            -- Если никогда не проводился VACUUM
                OR (live_rows > 100000 AND dead_rows / live_rows > 0.1) -- Если мертвые строки > 10%
        )
        SELECT 
            ts.schemaname, 
            ts.tablename, 
            ts.live_rows, 
            ts.dead_rows, 
            ts.last_vacuum, 
            ts.last_analyze
        FROM 
            tab_stats ts
        JOIN 
            frequent_upd fu
        ON 
            ts.schemaname = fu.schemaname
            AND ts.tablename = fu.tablename
        WHERE 
            ts.schemaname NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit')   -- системные схемы
            AND ts.schemaname NOT LIKE 'pg_toast%'                                    -- TOAST таблицы
            AND ts.schemaname NOT LIKE 'pg_temp%'                                     -- временные таблицы
            AND ts.live_rows > 500                                                  -- редко используемые таблицы с малым количеством данных
            AND ts.seq_scan > 50 --Исключаем редко используемые таблицы мало сканирований 
        ORDER BY 
            ts.schemaname, ts.tablename
        """)

        # Get the current timestamp in UTC as a timezone-aware datetime
        now = datetime.now(timezone.utc)

        rows = cur.fetchall()
        for row in rows:
            pschemaname, ptablename, live_rows, dead_rows, plast_vacuum, plast_analyze = row

            query = None
            # Perform ANALYZE if last_analyze is NULL or more than 1 day ago
            if (plast_analyze is None and plast_vacuum is None) or (
                    (plast_analyze is not None and now - plast_analyze > analyze_threshold) and (
                    plast_vacuum is not None and now - plast_vacuum > vacuum_threshold)):
                query = sql.SQL("VACUUM ANALYZE {}.{}").format(
                    sql.Identifier(pschemaname), sql.Identifier(ptablename))
                print(f"Running VACUUM ANALYZE on {pschemaname}.{ptablename}")
            elif plast_analyze is None or (plast_analyze is not None and now - plast_analyze > analyze_threshold):
                query = sql.SQL("ANALYZE {}.{}").format(
                    sql.Identifier(pschemaname), sql.Identifier(ptablename))
                print(f"Running ANALYZE on {pschemaname}.{ptablename}")
            # Perform VACUUM if last_vacuum is NULL or more than 5 days ago
            elif plast_vacuum is None or (plast_vacuum is not None and now - plast_vacuum > vacuum_threshold):
                query = sql.SQL("VACUUM {}.{}").format(
                    sql.Identifier(pschemaname), sql.Identifier(ptablename))
                print(f"Running VACUUM on {pschemaname}.{ptablename}")

            # Execute the query if one is defined
            if query:
                cur.execute(query)

        print("All tables analyzed or vacuumed.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    collect_stat_and_vacuum_for_missing_tables()
