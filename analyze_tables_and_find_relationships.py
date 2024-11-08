import time

import psycopg2
from psycopg2 import sql, errors
import secr

def log_execution_time(cur, relationship_id, query_type, execution_time, query_text):
    insert_query = """
    INSERT INTO public.query_execution_times (relationship_id, query_type, execution_time, query_text)
    VALUES (%s, %s, %s, %s)
    """
    cur.execute(insert_query, (relationship_id, query_type, execution_time, query_text))

def analyze_tables_and_find_relationships(conn):
    try:
        with conn.cursor() as cur:
            # Step 1: Analyze tables
            conn.autocommit = True
            analyze_query = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            AND table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            AND table_name NOT IN (
                SELECT relname FROM pg_class WHERE relkind = 'f'
            )
            AND table_name IN (
                SELECT relname FROM pg_class WHERE reltuples = -1
            )
            """
            start_time = time.time()
            cur.execute(analyze_query)
            tables_to_analyze = cur.fetchall()
            analyze_execution_time = time.time() - start_time
            log_execution_time(cur, None, "FETCH_ANALYZE_TABLES", analyze_execution_time, analyze_query)

            # Analyze each table
            for schema, table in tables_to_analyze:
                analyze_sql = sql.SQL("ANALYZE {}.{}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table)
                )
                print(f"Analyzing table: {schema}.{table}")

                start_time = time.time()
                cur.execute(analyze_sql)
                analyze_execution_time = time.time() - start_time
                log_execution_time(cur, None, "ANALYZE", analyze_execution_time, analyze_sql.as_string(conn))

            # Step 2: Find relationships
            find_columns_query = """
            SELECT DISTINCT pg_tables.schemaname, pg_tables.tablename, columns.column_name
            FROM pg_tables
            JOIN information_schema.columns 
            ON pg_tables.tablename = columns.table_name AND pg_tables.schemaname = columns.table_schema
            JOIN 
            (select pg_class.oid, pg_class.relname, np.nspname from pg_class
             JOIN pg_namespace np
             ON pg_class.relnamespace = np.oid) cl
            ON pg_tables.tablename = cl.relname  and cl.nspname = columns.table_schema 
            LEFT JOIN pg_exttable
			ON cl.oid = pg_exttable.reloid
            WHERE pg_tables.schemaname NOT IN ('pg_catalog', 'information_schema')
            AND pg_tables.schemaname NOT LIKE 'pg_toast%'
            AND pg_tables.schemaname NOT LIKE 'pg_temp%'
            AND pg_tables.tablename NOT LIKE '%_prt_%'  
            AND pg_tables.tablename NOT LIKE '%_tmp'
            AND pg_tables.tablename NOT LIKE '%tmp_'
            AND pg_tables.tablename NOT LIKE '_ak%'
            and tableowner =  'gpadmin'           
            AND pg_exttable.reloid IS NULL  
           -- and pg_tables.tablename= 'ln_loan_guar_desc_m' and pg_tables.schemaname = 'ext__fido'
            and columns.column_name not in ('dwh_job_id', 'dwh_created_at')
            """
            start_time = time.time()
            cur.execute(find_columns_query)
            columns = cur.fetchall()
            find_columns_execution_time = time.time() - start_time
            log_execution_time(cur, None, "FETCH_COLUMNS", find_columns_execution_time, find_columns_query)

            print('ok2')
            for schema1, table1, column1 in columns:
                for schema2, table2, column2 in columns:
                    # Check if relationship exists
                    check_existence_query = """
                    SELECT id FROM public.table_relationships
                    WHERE schema1_name = %s
                    AND table1_name = %s
                    AND column1_name = %s
                    AND schema2_name = %s
                    AND table2_name = %s
                    AND column2_name = %s
                    """
                    start_time = time.time()
                    print(f'.   check {schema1}.{table1}.{column1} - {schema2}.{table2}.{column2} - {str(start_time)}...', end = '')
                    cur.execute(check_existence_query, (schema1, table1, column1, schema2, table2, column2))
                    relationship_id_row = cur.fetchone()
                    check_existence_execution_time = time.time() - start_time
                    log_execution_time(cur, None, "CHECK_EXISTENCE", check_existence_execution_time, check_existence_query)
                    print('ok')

                    if relationship_id_row:
                        relationship_id = relationship_id_row[0]
                    else:
                        relationship_id = None  # No relationship found

                    # Skip if relationship exists or if comparing the same column
                    if relationship_id or (schema1, table1, column1) == (schema2, table2, column2):
                        continue

                    print(f'{schema1}.{table1}.{column1} - {schema2}.{table2}.{column2}...', end='')
                    # Check for matching relationship
                    relationship_query = sql.SQL("""
                    SELECT COUNT(*) > 0
                    FROM {schema1}.{table1} t1
                    JOIN {schema2}.{table2} t2
                    ON md5(COALESCE(t1.{column1}::text, '')) = md5(COALESCE(t2.{column2}::text, ''))
                    LIMIT 1
                    """).format(
                        schema1=sql.Identifier(schema1),
                        table1=sql.Identifier(table1),
                        schema2=sql.Identifier(schema2),
                        table2=sql.Identifier(table2),
                        column1=sql.Identifier(column1),
                        column2=sql.Identifier(column2)
                    )

                    start_time = time.time()
                    cur.execute("SET statement_timeout = 20000;")
                    try:
                        cur.execute(relationship_query)
                        match_found = cur.fetchone()[0]
                        flag = 1 if match_found else 0
                    except errors.QueryCanceled as e:
                        print("Query canceled due to statement timeout.")
                        flag = 2
                    relationship_execution_time = time.time() - start_time
                    log_execution_time(cur, None, "CHECK_RELATIONSHIP", relationship_execution_time, relationship_query.as_string(conn))



                    # Insert new relationship
                    insert_query = """
                    INSERT INTO public.table_relationships (schema1_name, table1_name, column1_name, schema2_name, table2_name, column2_name, flag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                    """
                    start_time = time.time()
                    cur.execute(insert_query, (schema1, table1, column1, schema2, table2, column2, flag))
                    insert_execution_time = time.time() - start_time
                    new_relationship_id = cur.fetchone()[0]
                    txt_query = f"""
                        INSERT INTO public.table_relationships (schema1_name, table1_name, column1_name, schema2_name, table2_name, column2_name, flag)
                        VALUES ('{schema1}', '{table1}', '{column1}', '{schema2}', '{table2}', '{column2}', {flag})
                    """
                    log_execution_time(cur, new_relationship_id, "INSERT_RELATIONSHIP", insert_execution_time, txt_query)

                    conn.commit()
                    print('ok')
            conn.commit()
            print("All necessary tables have been analyzed.")
    except Exception as e:
        print(f"Error: {str(e)}")
        conn.rollback()

# Database connection setup
def main():
    try:
        conn = psycopg2.connect(
            host="10.122.3.134",
            port="5432",
            database="greenplum-dwh",
            user="gpadmin",
            password=secr.pss()
        )
        print('ok1')
        analyze_tables_and_find_relationships(conn)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()


# ''''drop table public.query_execution_times;
# CREATE TABLE public.query_execution_times (
#     id SERIAL,
#     relationship_id INT,
#     query_type VARCHAR(50),  -- e.g., "ANALYZE", "CHECK_RELATIONSHIP"
#     execution_time NUMERIC(10, 4),  -- Time in seconds
#     query_text TEXT,  -- Stores the actual SQL query executed
#     executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# )
# with (appendonly = true, orientation = row)
# DISTRIBUTED BY (id);
#
# drop TABLE public.table_relationships;
# CREATE TABLE public.table_relationships (
# 	id serial4 NOT NULL,
# 	schema1_name varchar(255) NOT NULL,
# 	table1_name varchar(255) NOT NULL,
# 	column1_name varchar(255) NOT NULL,
# 	schema2_name varchar(255) NOT NULL,
# 	table2_name varchar(255) NOT NULL,
# 	column2_name varchar(255) NOT NULL,
# 	flag int4 DEFAULT 0 NOT NULL,
# 	created_at timestamp DEFAULT now() NULL
# )
# with (appendonly = true, orientation = row)
# DISTRIBUTED BY (id);'''