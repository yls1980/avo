import time

import psycopg2
from psycopg2 import sql, errors

import secr

def load_checked_pairs(file_path="checked_columns.txt"):
    checked_pairs = set()
    try:
        with open(file_path, "r") as file:
            for line in file:
                checked_pairs.add(line.strip())
    except FileNotFoundError:
        pass
    return checked_pairs

def save_checked_pair(schema1, table1, column1, schema2, table2, column2, file_path="checked_columns.txt"):
    with open(file_path, "a") as file:
        file.write(f"{schema1}.{table1}.{column1} - {schema2}.{table2}.{column2}\n")


def find_equal_values_in_tables(conn, file_path="checked_columns.txt"):
    try:
        checked_pairs = load_checked_pairs(file_path)
        with conn.cursor() as cur:
            tables_to_check = [
                ("ods__avo_cards", "cards"),
                ("ods__avo_internal_cards", "card"),
                ("ods__tranzaxis", "avo_token"),
                ("ods__tranzaxis", "avo_contract"),
                ("ods__tranzaxis", "avo_card"),
                ("ods__tranzaxis", "avo_card_product"),
                ("ods__tranzaxis", "avo_token_product")
            ]

            columns = []
            for schema, table in tables_to_check:
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    AND data_type NOT IN ('jsonb','boolean', 'ARRAY', 'json', 'date', 'timestamp', 'timestamp with time zone', 'timestamp without time zone')
                """, (schema, table))
                columns += [(schema, table, row[0]) for row in cur.fetchall()]

            total_comparisons = len(columns) * (len(columns) - 1) // 2
            i = 0
            for (schema1, table1, column1) in columns:
                for (schema2, table2, column2) in columns:
                    if (schema1, table1, column1) >= (schema2, table2, column2):
                        continue
                    i += 1
                    pair_key = f"{schema1}.{table1}.{column1} - {schema2}.{table2}.{column2}"
                    if pair_key in checked_pairs:
                        continue

                    compare_query = sql.SQL("""
                    SELECT COUNT(*) > 0
                    FROM (select * from {schema1}.{table1} where {column1} is not null order by {column1} limit 1000) t1
                    JOIN (select * from {schema2}.{table2} where {column2} is not null order by {column2} limit 1000) t2
                    ON md5(COALESCE(t1.{column1}::text, '')) = md5(COALESCE(t2.{column2}::text, ''))
                    where length(t1.{column1}::text)>10
                    and length(t2.{column2}::text)>10
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
                    try:
                        #print(f"                                  {schema1}.{table1}.{column1} and {schema2}.{table2}.{column2}")
                        save_checked_pair(schema1, table1, column1, schema2, table2, column2, file_path)
                        cur.execute("SET statement_timeout = 50000;")
                        cur.execute(compare_query)
                        match_found = cur.fetchone()[0]
                        if match_found:
                            print(f"{i}/{total_comparisons}")
                            print(f"Match found: {schema1}.{table1}.{column1} = {schema2}.{table2}.{column2}")
                    except Exception as e:
                        print(
                            f"error {schema1}.{table1}.{column1} and {schema2}.{table2}.{column2}   {str(e)}")
                        cur.execute("rollback")
                        raise

            print("Completed finding columns with equal values across tables.")
    except Exception as e:
        print(f"Error: {str(e)}")

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
        find_equal_values_in_tables(conn)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()