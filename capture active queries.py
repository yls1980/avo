# Скрипты для сборастатистики по активным запросам. Временное решение до установка базы мониторнга (gpperfmon)


#ddl table
drop table public.captured_queries;
CREATE TABLE public.captured_queries (
    query_id SERIAL,
    query TEXT,
    query_start TIMESTAMP,
    application_name TEXT,
    user_name TEXT,
    database_name TEXT,
    state TEXT,
    capture_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    flag int4
)
    WITH (
        appendoptimized=true, 
        orientation=column, 
        compresstype=zstd, 
        compresslevel=1
    )
    DISTRIBUTED BY (query_id)
;


# ddl function
CREATE OR REPLACE FUNCTION capture_active_queries() RETURNS VOID AS $$
BEGIN
    INSERT INTO public.captured_queries (query, query_start, application_name, user_name, database_name, state)
    SELECT query, query_start, application_name, usename, datname, state
    FROM pg_stat_activity
    WHERE state != 'idle'
      AND query != '<IDLE>';  
END;
$$ LANGUAGE plpgsql;



# code for dag
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'capture_active_queries_dag',
    default_args=default_args,
    description='A DAG to capture active queries every minute from Greenplum',
    schedule_interval='* * * * *',  # every minute
    catchup=False,
)

def capture_active_queries():
    try:
        gp_hook = PostgresHook(postgres_conn_id='gp_conn_id')
        
        sql_query = "SELECT capture_active_queries();"          
        connection = gp_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql_query)        

        cursor.close()
        connection.close()

    except Exception as e:
        print(f"Error occurred while capturing queries: {e}")

# Define the PythonOperator to call the capture_active_queries function
capture_queries_task = PythonOperator(
    task_id='capture_active_queries_task',
    python_callable=capture_active_queries,
    dag=dag,
)

capture_queries_task