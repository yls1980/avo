import io

import psycopg2
import pickle

# Database connection parameters
db_config = {
    'dbname': 'airflow_v2_db',
    'user': 'dwh_etl',
    'password': 'HMBG7umhhJits9IJaAQL40RQUBEif4rQ',
    'host': '10.122.3.224',
    'port': '5001'
}

def fetch_and_decode_conf():
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                query = """
                SELECT conf
                FROM public.dag_run dr
                WHERE run_type = 'manual'
                and id = 62
                AND conf IS NOT NULL;
                """
                cursor.execute(query)
                rows = cursor.fetchall()

                for row in rows:
                    conf_data = row[0]
                    try:
                        print(str(conf_data))
                        decoded_conf = pickle.loads(conf_data)
                        file = io.BytesIO(conf_data)
                        print(file)
                        print("Decoded conf:", decoded_conf)
                    except pickle.UnpicklingError as e:
                        print(f"Error decoding with pickle: {e}. Raw data: {conf_data}")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    fetch_and_decode_conf()
