import random

import psycopg2
import time
import logging
from secr import pss, pss2

# Configure logging
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.CRITICAL)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("execution_log.log"),
        console_handler
    ]
)

def fetch_queries_with_metadata(conn):
    """Fetch queries with progress metadata from temp.cb_test."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, query, row_number() OVER (ORDER BY NULL) AS rn, count(*) OVER () AS n_all
                from temp.cb_test
                where plan is null
                order by random() limit 900;
                """
            )
            queries = cursor.fetchall()
        logging.info("Fetched %d queries with metadata from temp.cb_test", len(queries))
        return queries
    except Exception as e:
        logging.error("D_Error fetching queries with metadata: %s", e)
        return []

def get_query_plan(conn, query):
    try:
        with conn.cursor() as cursor:
            a = random.randint(1,1000)
            if a % 16 == 0:
                cursor.execute(f"EXPLAIN ANALYZE{query}")
            else:
                cursor.execute(f"EXPLAIN (ANALYZE FALSE, VERBOSE TRUE) {query}")
            plan = cursor.fetchall()
        logging.info("Obtained query plan for query:\n %s", query)
        return "\n".join(row[0] for row in plan)
    except Exception as e:
        logging.error("D_Error obtaining query plan for query\n '%s'\n %s", f"EXPLAIN ANALYZE {query}", e)
        return None

def execute_query(conn, query):
    try:
        with conn.cursor() as cursor:
            start_time = time.time()
            #cursor.execute(f"SET statement_timeout = 300000;")
            cursor.execute(query)
            rowcount = cursor.rowcount
            end_time = time.time()
        execution_time = end_time - start_time
        logging.info("Executed query:\n %s\n, Time: %.2fs, Rows: %d", query, execution_time, rowcount)
        return execution_time, rowcount
    except Exception as e:
        logging.error("D_Error executing query\n '%s'\n %s", query, e)
        return None, 0

def save_execution_plan(conn, query, db_name, plan, execution_time, rowcount):
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO temp.cb_test (query, db_name, plan, execution_time, rowcount)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (query, db_name, plan, execution_time, rowcount),
            )
        logging.info("Saved execution plan for query: %s in DB:\n %s", query, db_name)
    except Exception as e:
        logging.error("D_Error saving execution plan for query\n '%s'\n %s", query, e)

def update_execution_plan(conn, query, db_name, plan, execution_time, rowcount, nid):
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE temp.cb_test
                SET db_name = %s, plan = %s, execution_time = %s, rowcount = %s
                WHERE id = %s
                """,
                (db_name, plan, execution_time, rowcount, nid),
            )
        logging.info("Updated execution plan for query: %s in DB:\n %s", query, db_name)
    except Exception as e:
        logging.error("D_Error updating execution plan for query\n'%s'\n %s", query, e)

def modify_text(text):
    text = text.rstrip()
    if text.endswith(';'):
        text = text[:-1] + ' LIMIT 1000;'
    else:
        text += '\nLIMIT 1000'

    return text

def err_replace(qtext):
    return qtext.replace(': current transaction is aborted, commands ignored until end of transaction block','')

def compare_databases(db1_conn, db2_conn):
    queries_with_metadata = fetch_queries_with_metadata(db2_conn)
    for id, query, rn, n_all in queries_with_metadata:
        nid = id
        print(f"Processing query {rn} of {n_all}")
        #logging.info("Processing query %d of %d: %s", rn, n_all, query)

        query = err_replace(query.replace(f'"{db1_conn.get_dsn_parameters()["dbname"]}".', ''))
        query1 = err_replace(query.replace(f'"{db2_conn.get_dsn_parameters()["dbname"]}".', ''))
        db1_plan = get_query_plan(db1_conn, query)
        db2_plan = get_query_plan(db2_conn, query1)

        db1_time, db1_rowcount = execute_query(db1_conn, modify_text(query))
        db2_time, db2_rowcount = execute_query(db2_conn, modify_text(query1))

        if db1_time and db2_time:
            update_execution_plan(db2_conn, query, "DB1", db1_plan, db1_time, db1_rowcount,nid)
            save_execution_plan(db2_conn, query1, f"DB2.{nid}", db2_plan, db2_time, db2_rowcount)

            logging.info(
                "DB1 Execution Time: %.2fs, Rows: %d; DB2 Execution Time: %.2fs, Rows: %d",
                db1_time, db1_rowcount, db2_time, db2_rowcount
            )
        else:
            err = ''
            if not db1_time:
                err = ' DB1'
            if not db2_time:
                err  += ' DB2'
            conn_save = conn_cb()
            conn_save.autocommit=True
            update_execution_plan(conn_save, query, f"error:{err}", 'error', None, 0, nid)
            conn_save.close()

def conn_db():
    return connect_to_db('10.122.3.134', '5432', 'greenplum-dwh', "gpadmin", pss)

def conn_cb():
    return connect_to_db('10.122.0.41', '5432', 'dwh_cloudberrydb', "admin_user", pss2())

def connect_to_db(host, port, dbname, user, password):
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        logging.info("Successfully connected to database: %s", dbname)
        return conn
    except Exception as e:
        logging.error("D_Error connecting to database '%s': %s", dbname, e)
        return None

def main():
    db1_conn = conn_db()
    db2_conn = conn_cb()
    db1_conn.autocommit = True
    db2_conn.autocommit = True

    if db1_conn and db2_conn:
        compare_databases(db1_conn, db2_conn)

        db1_conn.close()
        db2_conn.close()
        logging.info("Closed database connections")

if __name__ == "__main__":
    main()