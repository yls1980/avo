import os.path
import glob
import re
import signal
from os import mkdir

import paramiko
from get_ddl import find_unique_combinations,conn_db,get_row_count
import psycopg2
from psycopg2 import sql

import paramiko
import sqlparse
from sqlfluff.core import Linter
import concurrent.futures
import time

def tables():
    tables = []
    # tables.append('greenplum_dwh.ods.buf.ods__fido__dwh_personal_accounts')
    # tables.append('greenplum_dwh.ods.datamart_cod.datamart_cod__tx_turnover')
    # tables.append('greenplum_dwh.ods.datamart_cod.datamart_cod__tx_fido_oper_day')
    # tables.append('greenplum_dwh.ods.ods__fido_cod.ods__fido_cod__day_operational_cod')
    # tables.append('greenplum_dwh.ods.ods__ktc.ods__ktc__overall_unavailable_reasons_p')
    # tables.append('greenplum_dwh.ods.ods__asterisk.ods__asterisk__agents_timetable')
    # tables.append('greenplum_dwh.ods.datamart_risks.datamart_risks__income_model_monitoring')
    # tables.append('greenplum_dwh.ods.ods__jira.ods__jira__tickets_custex')
    #tables.append('greenplum_dwh.ods.datamart_cod.datamart_cod__tx_turnover')
    tables.append('greenplum_dwh.ods.datamart_cod.datamart_cod__tx_fido_oper_day')
    tables.append('greenplum_dwh.ods.ods__fido_cod.ods__fido_cod__day_operational_cod')
    tables.append('greenplum_dwh.ods.ods__ktc.ods__ktc__overall_unavailable_reasons_p')
    tables.append('greenplum_dwh.ods.datamart_risks.datamart_risks__income_model_monitoring')
    tables.append('greenplum_dwh.ods.ods__asterisk.ods__asterisk__agents_timetable')
    tables.append('greenplum_dwh.ods.ods__jira.ods__jira__tickets_custex')
    return tables

process_table = []

not_unique_table = []

host = '10.122.3.134'
port = 22
username = 'root'
db_name = 'greenplum-dwh'

def get_table_ddl_via_su(host, port, username, db_name, table_name):
    try:
        ssh_key_path = r'/Users/avotech/.ssh/id_rsa'
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, port=port, username=username, key_filename=ssh_key_path)

        # Выполнение команды через su
        command = f'su - gpadmin -c "pg_dump -d {db_name} -t {table_name} --schema-only | grep -v -E \'(SET|SELECT|REVOKE|--|COLUMN)\'"'
        stdin, stdout, stderr = ssh.exec_command(command)

        ddl_output = stdout.read().decode('utf-8')
        error_output = stderr.read().decode('utf-8')

        if error_output:
            print(f"Error: {error_output}")
            return None

        return ddl_output
    except Exception as e:
        print(f"Error: {str(e)}")
        return None
    finally:
        ssh.close()

def find_files_with_glob(directory, pattern):
    # Recursively search for files in all subdirectories
    return glob.glob(f"{directory}/**/{pattern}", recursive=True)

def read_file(model):
    f_n = f'{model}.sql'
    dir = '/Users/avotech/yarik/GIT/greenplum-dwh/dbt/greenplum_dwh/models'
    files = find_files_with_glob(dir,f_n)
    if os.path.exists(files[0]):
        with open(files[0], 'r', encoding='utf-8') as file:
            f_content = file.read()
            print (f_content)

def generate_record_hash(schema, table_name, unique_combinations, conn):
    coalesce_columns = []

    with conn.cursor() as cur:
        # Fetch the data type for each unique field
        for column in unique_combinations:
            cur.execute("""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """, (schema, table_name, column))
            col_type = cur.fetchone()[0]

            # Apply appropriate COALESCE based on the data type
            if col_type in ['text', 'character varying', 'character', 'name']:
                coalesce_columns.append(
                    sql.SQL("COALESCE({}, '')").format(sql.Identifier(column)))  # For text types
            elif col_type == 'boolean':
                coalesce_columns.append(
                    sql.SQL("COALESCE({}, FALSE)").format(sql.Identifier(column)))  # For boolean types
            elif col_type == 'bit':
                coalesce_columns.append(
                    sql.SQL("COALESCE({}, B'0')").format(sql.Identifier(column)))  # For bit types
            elif col_type == 'interval':
                coalesce_columns.append(
                    sql.SQL("COALESCE({}, '0'::interval)").format(sql.Identifier(column)))  # For interval types
            elif col_type in ['int4', 'int8', 'bigint', 'integer', 'smallint', 'oid']:
                coalesce_columns.append(
                    sql.SQL("COALESCE({}, 0)").format(sql.Identifier(column)))  # For integer types
            elif col_type in ['numeric', 'real', 'double precision']:
                coalesce_columns.append(
                    sql.SQL("COALESCE({}, 0)").format(sql.Identifier(column)))  # For numeric types
            elif col_type == 'uuid':
                coalesce_columns.append(
                    sql.SQL("COALESCE({}, '00000000-0000-0000-0000-000000000000')").format(
                        sql.Identifier(column)))  # For UUID
            elif col_type in ['timestamp without time zone', 'timestamp with time zone']:
                coalesce_columns.append(
                    sql.SQL("COALESCE({}, '1970-01-01 00:00:00')").format(sql.Identifier(column)))  # For timestamps
            elif col_type == 'time without time zone':
                coalesce_columns.append(
                    sql.SQL("COALESCE({}, '00:00:00')").format(sql.Identifier(column)))  # For time types
            elif col_type == 'inet':
                coalesce_columns.append(
                    sql.SQL("COALESCE({}, '0.0.0.0/0')").format(sql.Identifier(column)))  # For inet types
            elif col_type == 'bytea':
                coalesce_columns.append(
                    sql.SQL("COALESCE({}, ''::bytea)").format(sql.Identifier(column)))  # For bytea types
            elif col_type == 'ARRAY':
                coalesce_columns.append(
                    sql.SQL("COALESCE({}, '{{}}')").format(sql.Identifier(column)))  # For array types
            elif col_type in ['json', 'jsonb']:
                coalesce_columns.append(
                    sql.SQL("COALESCE({}, '{{}}'::jsonb)").format(sql.Identifier(column)))  # For JSON/JSONB
            elif col_type in ['pg_lsn', 'xid', 'abstime', 'regproc', 'pg_node_tree', '"char"']:
                coalesce_columns.append(
                    sql.SQL("COALESCE({}, '')").format(sql.Identifier(column)))  # For special types
            elif col_type == 'date':
                coalesce_columns.append(sql.SQL("COALESCE({}, '1970-01-01')").format(sql.Identifier(column)))
            else:
                coalesce_columns.append(
                    sql.SQL("COALESCE({}, '')").format(sql.Identifier(column)))  # Default for other types

        # Build the MD5 hash SQL expression using CONCAT instead of CONCAT_WS
        concat_columns = sql.SQL("MD5(CONCAT({}))::uuid").format(sql.SQL(", ").join(coalesce_columns))

        # Return the generated SQL query
        return concat_columns.as_string(conn)


import os


def get_processed_tables(file_name='processed_tables.txt'):
    """
    Reads the list of already processed tables from a file.
    Returns a set of processed table names.
    """
    if os.path.exists(file_name):
        with open(file_name, 'r', encoding='utf8') as f:
            processed_tables = set(line.strip() for line in f)
    else:
        processed_tables = set()

    return processed_tables


def update_processed_tables(table_name, file_name='processed_tables.txt'):
    """
    Adds a table to the processed tables list and writes it to the file.
    """
    with open(file_name, 'a', encoding='utf8') as f:
        f.write(table_name + '\n')

def ddl_upd_var1(ddl):
    if ddl.find('WITH')>-1:
        return ddl
    if ddl.upper().find('DISTRIBUTED RANDOMLY') > -1:
        ddl = ddl.replace('DISTRIBUTED RANDOMLY',
                          'WITH (appendonly = true,\norientation = column,\ncompresstype = zstd,\ncompresslevel = 1) \n DISTRIBUTED RANDOMLY')
    if ddl.upper().find('DISTRIBUTED BY') > -1:
        ddl = ddl.replace('DISTRIBUTED BY',
                          f'\nWITH (appendonly = true,\norientation = column,\ncompresstype = zstd,\ncompresslevel = 1) \n DISTRIBUTED RANDOMLY; -- ')
    return ddl

def process_tables():
    a = 0
    processed_tables_file = 'processed_tables.txt'
    processed_tables = get_processed_tables(processed_tables_file)

    f1 = 'common_table_refresh_scew.sql'
    f2 = 'answer_table.txt'

    # if os.path.exists(f1):
    #     os.remove(f1)
    # if os.path.exists(f2):
    #     os.remove(f2)

    fcommon_file = open(f1, 'a', encoding='utf8')
    fanswer_file = open(f2, 'a', encoding='utf8')

    try:
        process_tab = []
        conn = conn_db()
        for table in tables():
            make = False
            drop_old = ''
            comment_row = ''
            srecord_hash = ''
            if table in processed_tables:
                print(f'Skipping {table}, already processed.')
                continue  # Skip already processed tables

            a += 1
            try:
                bd, lay1, lay2, model = table.split('.')
                try:
                    t1, t2, t3 = model.split('__')
                    sch = t1 + '__' + t2
                except ValueError:
                    sch, t3 = model.split('__')
                tab = t3
            except ValueError:
                bd, lay1, model = table.split('.')
                sch, tab = model.split('__')

            table_name = sch + '.' + tab
            if table_name in process_table:
                print(f'Skipping {table}, already processed.')
                continue

            if lay1 == 'datamart' and 1==0:
                fanswer_file.write(f'Витрина {table_name}?')
                update_processed_tables(table, processed_tables_file)
                continue
            if len(sch.split('__'))>1:
                path_sch = sch.split('__')[1]
                ods_path = os.path.join(rf'/Users/avotech/yarik/GIT/greenplum-dwh/ddl', lay1, path_sch)
            else:
                path_sch = sch
                ods_path = os.path.join(rf'/Users/avotech/yarik/GIT/greenplum-dwh/ddl', path_sch)


            file_name = f'{sch}__{tab}.sql'
            os.makedirs(ods_path, exist_ok=True)
            file_name = os.path.join(ods_path, file_name)
            if os.path.exists(file_name):
                print(f'change {file_name}')

            print(f'get row count {table_name}...', end='')
            row_count = get_row_count(conn, sch, tab)
            print(f'-{row_count}.ok')

            make = True
            if (row_count < 1000000 and sch != 'dictionary') or (row_count >= 1000000 and sch == 'dictionary'):
                if (sch != 'dictionary' and row_count < 1000000 and not sch.startswith('stg__')):
                    comment_row = f'Маленькая непонятная таблица {table_name} row_count = {row_count}'
                elif sch.startswith('stg__'):
                    comment_row = f'STG таблица {table_name} row_count = {row_count}'
                elif (row_count >= 1000000 and sch == 'dictionary'):
                    comment_row = f'Таблица справочник, но слишком большая {table_name} row_count = {row_count}'
                print(f'Is {table_name} a dictionary or reference table? Row count = {row_count}?\n')
                fanswer_file.write(f'Is {table_name} a dictionary or reference table? Row count = {row_count}?')
            #elif 1==0:

            if make:
                print(f'get ddl table {table_name}...', end='')
                ddl = get_table_ddl_via_su(host, port, username, db_name, table_name)
                if ddl.find('DISTRIBUTED REPLICATED') > -1:
                    print(f'Skipping {table}, already processed.')
                    make = False
                if ddl:
                    print(f'ok')
                else:
                    comment_row = f'error in get ddl {table_name}'
                    print(f'ERR')
                    continue

                if ddl.find('DISTRIBUTED RANDOMLY')>-1:
                    drop_old = f'drop table if exists {sch}.{tab}_old;'
                if sch.startswith('stg__') and ddl.upper().find('DISTRIBUTED BY')>-1 and ddl.find('WITH')>-1:
                    pass
                elif sch.startswith('stg__') and ddl.upper().find('DISTRIBUTED BY') > -1 and ddl.find('WITH') == -1:
                    ddl = ddl.replace('DISTRIBUTED BY',
                                      'WITH (appendonly = true,\norientation = column,\ncompresstype = zstd,\ncompresslevel = 1) \n DISTRIBUTED BY')
                elif sch.startswith('stg__') and ddl.upper().find('DISTRIBUTED RANDOMLY') > -1 and ddl.find('WITH') == -1:
                    ddl = ddl.replace('DISTRIBUTED RANDOMLY',
                                      'WITH (appendonly = true,\norientation = column,\ncompresstype = zstd,\ncompresslevel = 1) \n DISTRIBUTED RANDOMLY')
                else:
                    ddl = ddl.replace('WITH (',
                                  '-- WITH (')
                if (sch == 'dictionary' or row_count<=10000) and not sch.startswith('stg__'):
                    comment_row = f'Таблица справочник {table_name} row_count = {row_count}'
                    ddl = ddl.replace('DISTRIBUTED RANDOMLY',
                                      'WITH (appendonly = true,\norientation = column,\ncompresstype = zstd,\ncompresslevel = 1) \n DISTRIBUTED REPLICATED')
                    ddl = ddl.replace('DISTRIBUTED BY',
                                  'WITH (appendonly = true,\norientation = column,\ncompresstype = zstd,\ncompresslevel = 1) \n DISTRIBUTED BY')
                elif table_name in not_unique_table:
                    comment_row = f'Таблица stg__ {table_name} row_count = {row_count}'
                    ddl = ddl_upd_var1(ddl)
                else:
                    if row_count>0:
                        max_len_combination_fields = None
                        #if row_count>3000000:
                        max_len_combination_fields=2
                        if f'{sch}.{tab}' in ('ods__fido.dwh_personal_accounts','datamart_cod.tx_turnover'):
                            buf = []
                        else:
                            buf = find_unique_combinations(conn, sch, tab,max_len_combination_fields)
                        if len(buf) == 0:
                            ddl = ddl_upd_var1(ddl)
                        else:
                            unique_combinations = buf[0]
                            if len(unique_combinations) == 1:
                                comment_row = f'Таблица c уникальным ключом {table_name} уникальное поле:{str(unique_combinations)} строк:{row_count}'
                                uniq_column = str(unique_combinations[0])
                                if ddl.find('DISTRIBUTED RANDOMLY') > -1:
                                    ddl = ddl.replace('DISTRIBUTED RANDOMLY',
                                                    f'\nWITH (appendonly = true,\norientation = column,\ncompresstype = zstd,\ncompresslevel = 1) \n DISTRIBUTED BY ({uniq_column})')
                                elif ddl.upper().find('DISTRIBUTED BY') > -1:
                                    ddl = ddl.replace('DISTRIBUTED BY',
                                                    f'\nWITH (appendonly = true,\norientation = column,\ncompresstype = zstd,\ncompresslevel = 1) \n DISTRIBUTED BY ({uniq_column}); -- ')
                            elif len(unique_combinations) > 1:
                                srecord_hash = generate_record_hash(sch, tab, unique_combinations, conn)
                                comment_row = f'Таблица без уникального ключа {table_name} уникальное поле:{srecord_hash} строк:{row_count}'
                                ddl = ddl.replace(')',
                                                  f',record_hash uuid)  \nWITH (appendonly = true,\norientation = column,\ncompresstype = zstd,\ncompresslevel = 1)')
                                ddl = ddl.replace('DISTRIBUTED RANDOMLY', f'DISTRIBUTED BY (record_hash)')
                                srecord_hash = ', '+srecord_hash

            if make:
                change_script = f"""
DO
$$
begin
 -- {comment_row}
 {drop_old}
 ALTER TABLE {table_name} RENAME TO {tab}_old;
 {ddl}
 insert into {table_name}
 select t.*{srecord_hash} from {sch}.{tab}_old t;
 analyze {table_name};
end;
$$
                """
                process_tab.append(table_name)
                fcommon_file.write(change_script)
                fcommon_file.write('\n\n')

                if ddl:
                    formatted_ddl = format_sql_with_timeout(ddl, 10)
                    if not formatted_ddl:
                        formatted_ddl = ddl

                    # Further beautify using SQLparse
                    beautified_ddl = clean_and_format_sql(formatted_ddl)
                    beautified_ddl = format_sql_with_sqlfluff(beautified_ddl)
                    pattern = r";\s*;"
                    beautified_ddl = re.sub(pattern, ";", beautified_ddl)
                    pattern = r"\sENCODING\s*\(.*?\)"
                    beautified_ddl = re.sub(pattern, "", beautified_ddl)
                    start_match = re.search(r'^\s*START\s*\(', beautified_ddl, re.MULTILINE)
                    if start_match:
                        start_indent = start_match.group(0).count(' ') - 1
                        beautified_ddl = re.sub(r'^\s*DEFAULT PARTITION', ' ' * start_indent + 'DEFAULT PARTITION',
                                                beautified_ddl,
                                                flags=re.MULTILINE)
                with open(file_name, 'w', encoding='utf8') as f:
                    f.write(beautified_ddl)

                print(f'{table_name} - ok. {file_name}, Rows = {row_count}')

                # Mark this table as processed
                update_processed_tables(table, processed_tables_file)

    finally:
        fcommon_file.close()
        fanswer_file.close()
        conn.close()
    print('\n'.join(process_tab))

def get_tables(conn, prefix='stg__'):
    """
    Get all table names where the schema starts with 'stg__'.
    """
    with conn.cursor() as cur:
        cur.execute(f"""
             SELECT table_schema, table_name 
             FROM information_schema.tables 
             WHERE table_schema LIKE '{prefix}%' AND table_type = 'BASE TABLE' and table_name not like '%_prt_%'
                and table_name not like '%_old'
                and table_name not like '%_bcp'
                and table_name not like '%_bkp'
                and table_name not like '%_bcp2'
                and table_name not like '%_bkp2'
                and table_name not like '%_tmp'
                and table_name not like 'tmp_%';
         """)
        return cur.fetchall()  # Returns a list of tuples (schema, table)


def format_sql_with_sqlfluff(ddl):
    linter = Linter(dialect='postgres')
    lint_result = linter.lint_string(ddl, fix=True)

    if lint_result.tree:
        fixed_sql = lint_result.tree.raw
        return fixed_sql
    else:
        return ddl

def format_sql_with_sqlparse(ddl):
    formatted_sql = sqlparse.format(ddl, reindent=True, keyword_case='upper')
    return formatted_sql


def replace_partitions(text: str) -> str:
    # Pattern to match the date intervals (START and END with EVERY)
    pattern = r"START\s+\('\d{4}-\d{2}-\d{2}'::date\)\s+END\s+\('\d{4}-\d{2}-\d{2}'::date\)\s+EVERY\s+\('\d+\s+\w+'::interval\),?\s*"

    # Replace all matches with #PARTITION#
    text = re.sub(pattern, "#PARTITION#", text)
    pattern = r"START\s+\('\d{4}-\d{2}-\d{2}.*?\)\s+END\s+\('\d{4}-\d{2}-\d{2}.*?\)\s+EVERY\s+\('.*?'::interval\)\s+WITH\s+\(tablename=.*?\)\s*"
    result = re.sub(pattern, "#PARTITION#,\n", text)

    return result

def extract_first_and_last_dates(ddl_text: str) -> str:
    # Find all START and END dates
    if ddl_text.find('ods__avo_registries.registries_records') >0:
        print("123")
    start_dates = re.findall(r"START\s+\('([\d-]+)'::date\)", ddl_text)
    end_dates = re.findall(r"END\s+\('([\d-]+)'::date\)", ddl_text)
    if not (start_dates and end_dates):
        start_dates = re.findall(r"START\s+\('([\d-]+\s[\d:]+(?:\+\d{2})?)'::(?:timestamp with time zone|date)\)", ddl_text)
        end_dates = re.findall(r"END\s+\('([\d-]+\s[\d:]+(?:\+\d{2})?)'::(?:timestamp with time zone|date)\)", ddl_text)

    if start_dates and end_dates:
        result = ddl_text
        pattern = r"START\s+\('(?P<start>\d{4}-\d{2}-\d{2})'::date\)\s+END\s+\('(?P<end>\d{4}-\d{2}-\d{2})'::date\)\s+EVERY\s+\('(?P<interval>\d+\s+\w+)'::interval\)"
        pattern1 = r"START\s+\('(?P<start>[\d-]+(?:\s+\d{2}:\d{2}:\d{2}\+\d{2})?)'::(?:date|timestamp with time zone)\)\s+END\s+\('(?P<end>[\d-]+(?:\s+\d{2}:\d{2}:\d{2}\+\d{2})?)'::(?:date|timestamp with time zone)\)\s+EVERY\s+\('(?P<interval>\d+\s+\w+)'::interval\)"
        pattern2 = r"START\s+\('([\d-]+\s+[\d:]+\+\d{2})'::timestamp with time zone\)\s+END\s+\('([\d-]+\s+[\d:]+\+\d{2})'::timestamp with time zone\)\s+EVERY\s+\('([\d\s\w]+)'::interval\)"

        matches = re.findall(pattern, ddl_text)
        first_res = None
        if matches:
            first_start = matches[0][0]  # The first start date
            last_end = matches[-1][1]  # The last end date
            interval = matches[0][2]  # The interval (assumed to be the same for all)

            first_res = f"START ('{first_start}'::date) END ('{last_end}'::date) EVERY ('{interval}'::interval)"
        else:
            matches = re.findall(pattern1, ddl_text)
            if matches:
                first_start = matches[0][0]  # The first start date/timestamp
                last_end = matches[-1][1]  # The last end date/timestamp
                interval = matches[0][2]  # The interval (assumed to be the same for all)
                first_res = f"START ('{first_start}'::timestamp with time zone) END ('{last_end}'::timestamp with time zone) EVERY ('{interval}'::interval)"
            else:
                matches = re.findall(pattern2, ddl_text)
                if matches:
                    first_start = matches[0][0]  # The first start date
                    last_end = matches[-1][1]  # The last end date
                    interval = matches[0][2]  # The interval (assumed to be the same for all)
                    result = f"START ('{first_start}'::timestamp with time zone) END ('{last_end}'::timestamp with time zone) EVERY ('{interval}'::interval)"

        if first_res:
            ddl_text = replace_partitions(ddl_text)
            pattern = r"WITH\s+\(tablename='[a-zA-Z0-9_]+',\s*appendonly='true',\s*compresstype=zstd,\s*compresslevel='1'\s*\)"
            ddl_text = re.sub(pattern, "", ddl_text)
            pattern = r"(#PARTITION#,)\s*,\s*"
            ddl_text = re.sub(pattern, r"\1", ddl_text)
            ddl_text = re.sub(r"(#PARTITION#,(\s*)?)+", f"#PARTITION#,\n", ddl_text)
            result = ddl_text.replace('#PARTITION#',first_res)
            pattern = r"WITH\s+\(tablename='[^']+',\s*appendonly='true',\s*orientation='[^']+',\s*compresstype=[^,]+,\s*compresslevel='[1-5]'\s*\)"
            result = re.sub(pattern, '', result)
        return result
    else:
        return ddl_text


def clean_and_format_sql(ddl: str) -> str:

    encoding_pattern = r"\sENCODING\s\(compresstype=(?:none|zstd|zlib),compresslevel=\d?,?blocksize=32768\)"
    cleaned_sql = re.sub(encoding_pattern, "", ddl)
    with_clause_pattern = r"\sWITH\s\(tablename='[^']+',\s*appendonly='true',\s*orientation='column',\s*compresstype=zstd,\s*compresslevel='1'\)"
    cleaned_sql = re.sub(with_clause_pattern, "", cleaned_sql)
    cleaned_sql = re.sub(r"\n\s*\n", "\n", cleaned_sql.strip())
    cleaned_sql = extract_first_and_last_dates(cleaned_sql)
    #cleaned_sql = re.sub(r"\s+", " ", cleaned_sql).strip()
    return cleaned_sql

TIMEOUT_SECONDS = 20

class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException()

def format_sql_with_timeout(ddl, timeout_seconds):
    """Wraps format_sql_with_sqlfluff to enforce a timeout and log the time taken."""
    start_time = time.time()  # Record the start time

    # Set up a signal to raise a timeout exception after timeout_seconds
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout_seconds)  # Start the timer

    try:
        formatted_ddl = format_sql_with_sqlfluff(ddl)  # Call the formatting function
    except TimeoutException:
        print(f"Formatting timed out after {timeout_seconds} seconds")
        formatted_ddl = None
    finally:
        signal.alarm(0)  # Disable the alarm

    end_time = time.time()  # Record the end time
    elapsed_time = end_time - start_time  # Calculate the elapsed time

    print(f"Time taken to format SQL: {elapsed_time:.2f} seconds")

    return formatted_ddl

def get_ddl_for_tables(prefix, tables = None):

    try:
        conn = conn_db()  # Assuming conn_db() establishes a connection to the database
        if not tables:
            stg_tables = get_tables(conn, prefix)  # Get all tables with schema names starting with 'stg__'
        else:
            stg_tables = [tuple(table.split(".")) for table in tables]
        ods_path = os.path.join(rf'/Users/avotech/yarik/GIT/greenplum-dwh/ddl')

        processed_tables_file = 'processed_tables_ddl.txt'
        if os.path.exists(processed_tables_file):
            with open(processed_tables_file, 'r', encoding='utf8') as f:
                processed_tables = set(line.strip() for line in f.readlines())
        else:
            processed_tables = set()

        for schema, table in stg_tables:
            table_name = f"{schema}.{table}"

            if table_name in processed_tables:
                print(f"Skipping already processed table: {table_name}")
                continue

            print(f'Fetching DDL for {table_name}...')
            ddl = get_table_ddl_via_su(host, port, username, db_name, table_name)

            if ddl:
                # Format using sqlfluff
                #formatted_ddl = format_sql_with_sqlfluff(ddl)
                formatted_ddl = format_sql_with_timeout(ddl, TIMEOUT_SECONDS)
                if not formatted_ddl:
                    formatted_ddl = ddl

                # Further beautify using SQLparse
                beautified_ddl = clean_and_format_sql(formatted_ddl)
                beautified_ddl = format_sql_with_sqlfluff(beautified_ddl)
                pattern = r";\s*;"
                beautified_ddl = re.sub(pattern, ";", beautified_ddl)
                pattern = r"\sENCODING\s*\(.*?\)"
                beautified_ddl = re.sub(pattern, "", beautified_ddl)
                start_match = re.search(r'^\s*START\s*\(', beautified_ddl, re.MULTILINE)
                if start_match:
                    start_indent = start_match.group(0).count(' ')-1
                    beautified_ddl = re.sub(r'^\s*DEFAULT PARTITION', ' ' * start_indent + 'DEFAULT PARTITION', beautified_ddl,
                                      flags=re.MULTILINE)

                # Print the beautified SQL
                print(f"Beautified DDL for {table_name}:\n{beautified_ddl}")
                file_name = f'{schema}__{table}.sql'
                lay = schema.split('__')[0]
                if len(schema.split('__'))>1:
                    folder_schema = schema.split('__')[1]
                    path_table = os.path.join(ods_path,lay,folder_schema)
                else:
                    path_table = os.path.join(ods_path,lay)
                os.makedirs(path_table, exist_ok=True)
                file_name = os.path.join(path_table, file_name)
                if os.path.exists(file_name):
                    print(f'change {file_name}')
                with open(file_name, 'w', encoding='utf8') as f:
                    f.write(beautified_ddl)
                with open(processed_tables_file, 'a', encoding='utf8') as f:
                    f.write(f"{table_name}\n")
            else:
                print(f"Error: Could not fetch DDL for {table_name}")



    finally:
        conn.close()

table_list = ['datamart_cod.tx_fido_oper_day',
'datamart_cod.tx_turnover',
'datamart_risks.income_model_monitoring',
'ods__asterisk.agents_timetable',
'ods__fido.dwh_personal_accounts',
'ods__fido_cod.day_operational_cod',
'ods__jira.tickets_custex',
'ods__ktc.overall_unavailable_reasons_p',]

get_ddl_for_tables('ods__',table_list)
#get_ddl_for_tables('stg__')
#process_tables()

# for table in tables():
#     try:
#         bd, lay1, lay2, model = table.split('.')
#         t1, t2, t3 = model.split('__')
#         sch = t1 + '__' + t2
#         tab = t3
#     except ValueError:
#         bd, lay1, model = table.split('.')
#         sch, tab = model.split('__')
#     print(f"""INSERT INTO public.tabs (table_name) VALUES ('{sch}.{tab}');""")
# for table in process_table:
#     print(f"""INSERT INTO public.proc_tabs (table_name) VALUES ('{table}');""")