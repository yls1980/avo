import psycopg2
import json
from datetime import datetime
import secr

# Database connection details
DB_CONFIG = {
    "dbname": "greenplum-dwh",
    "user": "gpadmin",
    "password": secr.pss(),
    "host": "10.122.3.134",
    "port": 5432
}

EXPLAIN_PLAN_TABLE = "public.explain_plans"

FETCH_QUERY = """
WITH data AS (
    SELECT 
        query, 
        usename, 
        MAX(query_age) AS age_query,
        (regexp_matches(
            query,
            $$create temporary table.*?as \(\s*(.*?)\s*\)\s+distributed randomly\s*;$$
        ))[1] AS extracted_sql
    FROM 
        public.captured_queries
    WHERE 
        query_start > NOW() - INTERVAL '5 days'
        AND query_age IS NOT NULL
        and md5(coalesce(query,''))::uuid not  in (select qhash from public.explain_plans)
    GROUP BY 
        query, usename
    HAVING 
        MAX(query_age) > INTERVAL '1 min'
)
SELECT 
    query, 
    usename, 
    age_query, 
    extracted_sql AS query_to_explain,
    md5(coalesce(query,''))::uuid qhash
FROM 
    data
ORDER BY 
    age_query DESC, 
    LENGTH(query) DESC;
"""


def save_explain_plan(plan_json, query_text, usename, age_query, qhash):
    insert_query = f"""
    INSERT INTO {EXPLAIN_PLAN_TABLE} (query_text, explain_plan, usename, age_query, created_at, qhash)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(insert_query, (query_text, json.dumps(plan_json), usename, age_query, datetime.now(),qhash))
            conn.commit()


def fetch_and_save_explain_plans():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(FETCH_QUERY)
                queries_to_explain = cur.fetchall()  # Fetch all rows

                for query, usename, age_query, extracted_sql,qhash in queries_to_explain:
                    try:
                        explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {extracted_sql}"
                        cur.execute(explain_query)
                        plan = cur.fetchone()[0]

                        save_explain_plan(plan, extracted_sql, usename, age_query, qhash)

                        print(f"Explain plan for query '{query}' saved successfully.")

                    except Exception as e:
                        print(f"Error processing query '{extracted_sql}': {e}")

    except Exception as e:
        print(f"Error fetching queries to explain: {e}")


if __name__ == "__main__":
    fetch_and_save_explain_plans()
