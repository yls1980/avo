import psycopg2
from psycopg2 import sql
from datetime import timedelta
import secr

# Database connection parameters
DB_CONFIG = {
    'host': "10.122.3.134",
    'port': "5432",
    'database': "greenplum-dwh",
    'user': "gpadmin",
    'password': secr.pss()
}

# SQL query to fetch distinct month starts from the source table
get_months_query = """
    SELECT DISTINCT date_trunc('month', created_at) AS month_start
    FROM ods__kafka.logger_events_bu20241007;
"""

# SQL queries for inserting data
insert_with_dates_query = """
    INSERT INTO ods__kafka.logger_events
    ("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
    "kafka_key", "kafka_value", "kafka_headers", "user_id", 
    "device_id", "device_os", "device_model", "created_at",
    "data_geoposition_lat_lon", "source_element", "source_screen", 
    "source_action", "dwh_job_id", "dwh_created_at", record_hash)        
    SELECT "kafka_topic", "kafka_partition", "kafka_offset", 
           "kafka_timestamp", "kafka_key", "kafka_value", "kafka_headers",
           "user_id", "device_id", "device_os", "device_model", 
           COALESCE("created_at", '1970-01-01'::timestamp) AS created_at, 
           "data_geoposition_lat_lon", "source_element", "source_screen", 
           "source_action", "dwh_job_id", "dwh_created_at",
           md5("kafka_topic" || "kafka_partition"::text || "kafka_offset"::text)::uuid AS record_hash
    FROM ods__kafka.logger_events_bu20241007
    WHERE created_at >= %s
    AND created_at < %s;
"""

insert_with_null_dates_query = """
    INSERT INTO ods__kafka.logger_events
    ("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
    "kafka_key", "kafka_value", "kafka_headers", "user_id", 
    "device_id", "device_os", "device_model", "created_at",
    "data_geoposition_lat_lon", "source_element", "source_screen", 
    "source_action", "dwh_job_id", "dwh_created_at", record_hash)        
    SELECT "kafka_topic", "kafka_partition", "kafka_offset", 
           "kafka_timestamp", "kafka_key", "kafka_value", "kafka_headers",
           "user_id", "device_id", "device_os", "device_model", 
           COALESCE("created_at", '1970-01-01'::timestamp) AS created_at,  
           "data_geoposition_lat_lon", "source_element", "source_screen", 
           "source_action", "dwh_job_id", "dwh_created_at",
           md5("kafka_topic" || "kafka_partition"::text || "kafka_offset"::text)::uuid AS record_hash
    FROM ods__kafka.logger_events_bu20241007
    WHERE created_at IS NULL;
"""

def insert_data_for_date_range(connection, start_date, end_date):
    """Inserts data for a specific date range."""
    try:
        with connection.cursor() as cursor:
            cursor.execute(insert_with_dates_query, (start_date, end_date))
            ins_count = cursor.rowcount
            connection.commit()
            print(f"Inserting for range: {start_date} to {end_date}, Inserted: {ins_count} records.")
    except Exception as e:
        connection.rollback()
        print(f"Error inserting data for range {start_date} to {end_date}: {e}")

def insert_data_with_null_dates(connection):
    """Inserts data where created_at is NULL."""
    try:
        with connection.cursor() as cursor:
            cursor.execute(insert_with_null_dates_query)
            ins_count = cursor.rowcount
            connection.commit()
            print(f"Inserting data with NULL created_at, Inserted: {ins_count} records.")
    except Exception as e:
        connection.rollback()
        print(f"Error inserting data with NULL dates: {e}")

def process_in_batches():
    """Process the insertion in batches by looping over distinct month starts."""
    try:
        connection = psycopg2.connect(**DB_CONFIG)

        with connection.cursor() as cursor:
            # Fetch distinct month starts from the source table
            cursor.execute(get_months_query)
            months = cursor.fetchall()

        # Loop through each distinct month and perform the batch insert
        for record in months:
            start_date = record[0]
            if start_date:
                end_date = start_date + timedelta(days=30)  # Use timedelta to add 1 month (approx. 30 days)
                insert_data_for_date_range(connection, start_date, end_date)
            else:
                insert_data_with_null_dates(connection)

    except Exception as e:
        print(f"Error processing batches: {e}")

    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    process_in_batches()
