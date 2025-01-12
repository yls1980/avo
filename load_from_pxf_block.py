import pandas as pd
import pyodbc
import psycopg2
from psycopg2 import extras
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import argparse
import secr

mssql_connection_string = secr.return_ms()

db_params = secr.return_gp()

# Query to fetch data from MSSQL
fetch_query_template = """
SELECT 
    kafka_topic, kafka_partition, kafka_offset, kafka_created_at, kafka_value,
    event_type, created_at, context_status_cl, context_status_rp, context_person_id,
    context_core_bank_client_id, context_credit_report_type, context_credit_report_body_report,
    context_cr_request_id, context_reject_reason_exp, context_acquier, context_tx_transaction_id,
    context_mcc, context_masked_card_number, context_merchant_name, context_tran_time,
    context_reject_reason, context_is_partial, context_commission, context_original_currency,
    context_terminal_desc, context_specification_id, context_terminal_coutry, context_amount,
    context_product_id, context_vat, context_is_advice, context_is_reversal, context_parent_transaction_id,
    context_reg_time, context_operation_trace_id, context_original_amount, context_terminal_city,
    context_card_id, context_ichg_fee_amt, context_life_phase, context_ichg_fee_currency, context_status,
    context_tx_command_id, context_tx_id, context_requested_cl, context_applied_cl, context_user_id,
    context_transaction_id, context_loan_id, context_bankomat_id, error_error_code, error_error_description,
    dwh_created_at, dwh_job_id
FROM raw_chiltan.kafka.tx_events
WHERE kafka_created_at >= ? AND kafka_created_at < ?;
"""

# Query to insert data into Greenplum
insert_query = """
INSERT INTO temp.tx_events (
    kafka_topic, kafka_partition, kafka_offset, kafka_created_at, kafka_value, event_type,
    created_at, context_status_cl, context_status_rp, context_person_id, context_core_bank_client_id,
    context_credit_report_type, context_credit_report_body_report, context_cr_request_id, context_reject_reason_exp,
    context_acquier, context_tx_transaction_id, context_mcc, context_masked_card_number, context_merchant_name,
    context_tran_time, context_reject_reason, context_is_partial, context_commission, context_original_currency,
    context_terminal_desc, context_specification_id, context_terminal_coutry, context_amount, context_product_id,
    context_vat, context_is_advice, context_is_reversal, context_parent_transaction_id, context_reg_time,
    context_operation_trace_id, context_original_amount, context_terminal_city, context_card_id, context_ichg_fee_amt,
    context_life_phase, context_ichg_fee_currency, context_status, context_tx_command_id, context_tx_id,
    context_requested_cl, context_applied_cl, context_user_id, context_transaction_id, context_loan_id,
    context_bankomat_id, error_error_code, error_error_description, dwh_created_at, dwh_job_id
)
SELECT * FROM (VALUES %s) AS temp_data (
    kafka_topic, kafka_partition, kafka_offset, kafka_created_at, kafka_value, event_type,
    created_at, context_status_cl, context_status_rp, context_person_id, context_core_bank_client_id,
    context_credit_report_type, context_credit_report_body_report, context_cr_request_id, context_reject_reason_exp,
    context_acquier, context_tx_transaction_id, context_mcc, context_masked_card_number, context_merchant_name,
    context_tran_time, context_reject_reason, context_is_partial, context_commission, context_original_currency,
    context_terminal_desc, context_specification_id, context_terminal_coutry, context_amount, context_product_id,
    context_vat, context_is_advice, context_is_reversal, context_parent_transaction_id, context_reg_time,
    context_operation_trace_id, context_original_amount, context_terminal_city, context_card_id, context_ichg_fee_amt,
    context_life_phase, context_ichg_fee_currency, context_status, context_tx_command_id, context_tx_id,
    context_requested_cl, context_applied_cl, context_user_id, context_transaction_id, context_loan_id,
    context_bankomat_id, error_error_code, error_error_description, dwh_created_at, dwh_job_id
)
WHERE NOT EXISTS (
    SELECT 1 FROM temp.tx_events e WHERE e.kafka_offset = temp_data.kafka_offset and e.kafka_created_at = temp_data.kafka_created_at 
);
"""
    
# Function to process a single hour
def process_single_hour(start_datetime, end_datetime):
    try:
        # Connect to MSSQL
        mssql_conn = pyodbc.connect(mssql_connection_string)
        mssql_cursor = mssql_conn.cursor()

        # Connect to Greenplum
        greenplum_conn = psycopg2.connect(**db_params)
        greenplum_conn.autocommit = True
        greenplum_cursor = greenplum_conn.cursor()

        # Fetch data from MSSQL for the current hour
        print(f"Fetching data for {start_datetime}...")
        mssql_cursor.execute(fetch_query_template, (start_datetime, end_datetime))
        rows = mssql_cursor.fetchall()

        if not rows:
            print(f"No data found for {start_datetime}.")
            return

        # Convert data to Pandas DataFrame
        columns = [column[0] for column in mssql_cursor.description]
        data = pd.DataFrame.from_records(rows, columns=columns)
        data = data.replace({'\x00': ''}, regex=True)
        print(f"Fetched {len(data)} rows for {start_datetime}.")

        # Insert data into Greenplum
        valid_rows = [tuple(row) for row in data.to_numpy()]
        extras.execute_values(greenplum_cursor, insert_query, valid_rows)
        print(f"Inserted {len(valid_rows)} rows into Greenplum for {start_datetime}.")

    except Exception as e:
        print(f"An error occurred for {start_datetime}: {e}")
    finally:
        # Close all connections
        if mssql_cursor:
            mssql_cursor.close()
        if mssql_conn:
            mssql_conn.close()
        if greenplum_cursor:
            greenplum_cursor.close()
        if greenplum_conn:
            greenplum_conn.close()

# Function to process data hour by hour using threads
def process_data_hour_by_hour(start_date, end_date, thread_count):
    current_datetime = start_date
    tasks = []

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        while current_datetime < end_date:
            next_datetime = current_datetime + timedelta(hours=1)
            tasks.append(executor.submit(process_single_hour, current_datetime, next_datetime))
            current_datetime = next_datetime

        for task in tasks:
            task.result()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process data hour by hour.")
    parser.add_argument("--start_date", type=str, required=True, help="Start datetime in YYYY-MM-DD HH:MM:SS format")
    parser.add_argument("--end_date", type=str, required=True, help="End datetime in YYYY-MM-DD HH:MM:SS format")
    parser.add_argument("--threads", type=int, required=True, help="Number of threads to use")

    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d %H:%M:%S")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d %H:%M:%S")
    thread_count = args.threads
    # start_date = datetime.strptime('2024-03-31 20:00:00', "%Y-%m-%d %H:%M:%S")
    # end_date = datetime.strptime('2024-04-01 00:00:00', "%Y-%m-%d %H:%M:%S")
    # thread_count = 14

    process_data_hour_by_hour(start_date, end_date, thread_count)


