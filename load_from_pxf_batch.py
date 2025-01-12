import psycopg2
import json

import ujson
from psycopg2 import extras

import secr

db_params = secr.db_params()

fetch_query = """
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
FROM temp.ext_tx_events t where not exists (select 1 from temp.tx_events tt where tt.kafka_offset = t.kafka_offset);
"""

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
) VALUES %s;
"""


def validate_json_fields(row):
    try:
        # Handle 'kafka_value'
        if row["kafka_value"] is not None:
            buf = row["kafka_value"]
            buf = buf.replace("}}",'}"}')
            json1 = ujson.loads(buf)
            row["kafka_value"] = json1
        else:
            row["kafka_value"] = {}

        if row["context_credit_report_body_report"] is not None:
            json1 = ujson.loads(row["context_credit_report_body_report"])
            row["context_credit_report_body_report"] = json1  # Update the row with parsed JSON
        else:
            row["context_credit_report_body_report"] = {}
        return True
    except (TypeError, ValueError, ujson.JSONDecodeError) as e:
        print(buf)
        #print(f"Value of 'context_credit_report_body_report': {row['context_credit_report_body_report']}")
        print(f"Error parsing JSON at kafka_offset={row.get('kafka_offset', 'Unknown')}: {e}")
        return False


def main():
    try:
        conn = psycopg2.connect(**db_params)
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute(fetch_query)
        rows = cursor.fetchall()

        valid_rows = []
        invalid_count = 0
        valid_count = 0
        for row in rows:
            row_dict = dict(zip([desc[0] for desc in cursor.description], row))

            if validate_json_fields(row_dict):
                valid_rows.append(row)
                valid_count += 1
            else:
                invalid_count += 1

        print(f"Number of valid rows: {len(valid_rows)}")
        print(f"Number of invalid rows: {invalid_count}")

        if valid_rows:
            extras.execute_values(cursor, insert_query, valid_rows)

        print("Valid rows inserted successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    main()
