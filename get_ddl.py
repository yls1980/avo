import itertools
import os.path
import re
import time
import traceback

import sqlparse
from sqlfluff.core import Linter

import paramiko
import psycopg2
from psycopg2 import sql
from secr import pss  

from multiprocessing import Pool
import signal


def get_schema_ddl_via_su(host, ssh_port, username, db_name, schema):
    ssh_key_path = r'/Users/avotech/.ssh/id_rsa'  # Update with your SSH key path

    try:
        # Set up the SSH connection
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, port=ssh_port, username=username, key_filename=ssh_key_path)

        print(f"Fetching DDL for schema: {schema}")
        command = (
            f"su - gpadmin -c \"pg_dump -d {db_name} --schema={schema} --schema-only\""
        )

        stdin, stdout, stderr = ssh.exec_command(command)

        ddl_output = stdout.read().decode('utf-8')
        error_output = stderr.read().decode('utf-8')

        if error_output:
            print(f"Error fetching DDL for schema {schema}: {error_output}")
            return f"Error fetching DDL for schema {schema}: {error_output}"
        else:
            return ddl_output

    except Exception as e:
        print(f"Error 0: {str(e)}")
        return f"Error 0: {str(e)}"

    finally:
        ssh.close()

# Получение DDL для таблицы через SSH
def get_table_ddl_via_su(host, port, username, db_name, table_name):
    try:
        ssh_key_path = r'/Users/avotech/.ssh/id_rsa'
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, port=port, username=username, key_filename=ssh_key_path)

        # Выполнение команды через su
        command = f'su - gpadmin -c "pg_dump -d {db_name} -t {table_name} --schema-only"'
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


# Подключение к базе данных PostgreSQL
def connect_to_db(host, port, dbname, user, password):
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    return conn


# Получение списка всех столбцов таблицы
def get_columns(conn, schema, table_name):
    with conn.cursor() as cur:
        query = sql.SQL("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s;
        """)
        cur.execute(query, (schema, table_name))
        columns = [row[0] for row in cur.fetchall()]
    return columns


# Получение количества уникальных значений для каждого столбца из статистики
def get_unique_value_counts(conn, schema, table_name, columns):
    unique_value_counts = {}

    with conn.cursor() as cur:
        query = sql.SQL("""
            SELECT attname, n_distinct
            FROM pg_stats
            WHERE schemaname = %s
            AND tablename = %s
            AND attname = ANY(%s)
            and attname not in ('dwh_job_id','dwh_created_at')           
            and null_frac = 0;
        """)

        # print (f'analyze {schema}.{table_name}...', end ='')
        # cur.execute(f'analyze {schema}.{table_name}')
        # print(f'ok')
        cur.execute(query, (schema, table_name, columns))
        stats = cur.fetchall()

        for stat in stats:
            column_name = stat[0]
            n_distinct = stat[1]
            if n_distinct < 0:
                query = sql.SQL("""
                    SELECT COUNT(*) FROM {schema}.{table}
                """).format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table_name)
                )
                cur.execute(query)
                total_rows = cur.fetchone()[0]
                n_distinct = abs(n_distinct) * total_rows

            unique_value_counts[column_name] = n_distinct

    return unique_value_counts

def is_unique_task(params):
    conn_params, schema, table_name, columns = params
    conn = connect_to_db(*conn_params)  # Устанавливаем соединение в каждом процессе
    result = is_unique(conn, schema, table_name, columns)
    conn.close()  # Закрываем соединение по завершении работы
    return columns, result

def is_unique(conn, schema, table_name, columns):
    with conn.cursor() as cur:
        coalesce_columns = []

        # Запрашиваем тип данных для каждого столбца
        for column in columns:
            cur.execute("""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """, (schema, table_name, column))
            col_type = cur.fetchone()[0]

            # Обработка типов данных с соответствующими значениями для NULL
            if col_type in ['text', 'character varying', 'character', 'name']:
                coalesce_columns.append(
                    sql.SQL("COALESCE({0}, '')").format(sql.Identifier(column)))  # Для текстовых типов
            elif col_type == 'boolean':
                coalesce_columns.append(
                    sql.SQL("COALESCE({0}, FALSE)").format(sql.Identifier(column)))  # Для булевых типов
            elif col_type == 'bit':
                coalesce_columns.append(
                    sql.SQL("COALESCE({0}, B'0')").format(sql.Identifier(column)))  # Для bit
            elif col_type == 'interval':
                coalesce_columns.append(
                    sql.SQL("COALESCE({0}, '0'::interval)").format(sql.Identifier(column)))  # Для interval
            elif col_type in ['int4', 'int8', 'bigint', 'integer', 'smallint', 'oid']:
                coalesce_columns.append(
                    sql.SQL("COALESCE({0}, 0)").format(sql.Identifier(column)))  # Для целых чисел
            elif col_type in ['numeric', 'real', 'double precision']:
                coalesce_columns.append(
                    sql.SQL("COALESCE({0}, 0)").format(sql.Identifier(column)))  # Для чисел с плавающей запятой
            elif col_type == 'uuid':
                coalesce_columns.append(
                    sql.SQL("COALESCE({0}, '00000000-0000-0000-0000-000000000000')").format(
                        sql.Identifier(column)))  # Для UUID
            elif col_type in ['timestamp without time zone', 'timestamp with time zone']:
                coalesce_columns.append(
                    sql.SQL("COALESCE({0}, '1970-01-01 00:00:00')").format(sql.Identifier(column)))  # Для timestamp
            elif col_type == 'time without time zone':
                coalesce_columns.append(
                    sql.SQL("COALESCE({0}, '00:00:00')").format(sql.Identifier(column)))  # Для time
            elif col_type == 'inet':
                coalesce_columns.append(
                    sql.SQL("COALESCE({0}, '0.0.0.0/0')").format(sql.Identifier(column)))  # Для inet
            elif col_type == 'bytea':
                coalesce_columns.append(
                    sql.SQL("COALESCE({0}, ''::bytea)").format(sql.Identifier(column)))  # Для bytea
            elif col_type == 'ARRAY':
                coalesce_columns.append(
                    sql.SQL("COALESCE({0}, '{{}}')").format(sql.Identifier(column)))  # Для массивов
            elif col_type in ['json', 'jsonb']:
                coalesce_columns.append(
                    sql.SQL("COALESCE({0}, '{{}}'::jsonb)").format(sql.Identifier(column)))  # Для json и jsonb
            elif col_type in ['pg_lsn', 'xid', 'abstime', 'regproc', 'pg_node_tree', '"char"']:
                coalesce_columns.append(
                    sql.SQL("COALESCE({0}, '')").format(sql.Identifier(column)))  # Для специальных типов
            elif col_type == 'date':
                coalesce_columns.append(sql.SQL("COALESCE({}, '1970-01-01')").format(sql.Identifier(column)))
            else:
                coalesce_columns.append(
                    sql.SQL("COALESCE({0}, '')").format(sql.Identifier(column)))  # По умолчанию для остальных типов

        # Используем CONCAT_WS для объединения значений колонок и вычисляем MD5-хэш
        concat_columns = sql.SQL("MD5(CONCAT_WS('|', {}))").format(sql.SQL(", ").join(coalesce_columns))

        query = sql.SQL("""
            SELECT COUNT(*) = COUNT(DISTINCT {concat_columns})
            FROM {schema}.{table};
        """).format(
            concat_columns=concat_columns,
            schema=sql.Identifier(schema),
            table=sql.Identifier(table_name)
        )

        cur.execute(query)
        result = cur.fetchone()[0]
        print(f'{columns}-{result}')
    return result


def run_is_unique_parallel(conn_params, schema, table_name, columns_combinations, num_procs=5):
    # Создаем пул процессов
    with Pool(processes=num_procs) as pool:
        # Подготавливаем параметры для каждой задачи
        tasks = [(conn_params, schema, table_name, columns) for columns in columns_combinations]

        # Запускаем задачи параллельно
        results = pool.map(is_unique_task, tasks)

        # Обрабатываем результаты
        for columns, is_unique_result in results:
            print(f"Комбинация {columns}: уникальная - {is_unique_result}")




# Поиск уникальных комбинаций полей
# def find_unique_combinations(conn, schema, table_name):
#     columns = get_columns(conn, schema, table_name)
#     unique_value_counts = get_unique_value_counts(conn, schema, table_name, columns)
#     sorted_columns = sorted(unique_value_counts, key=unique_value_counts.get, reverse=True)
#     unique_combinations = []
#
#     # Проверка каждого столбца отдельно
#     for column in sorted_columns:
#         if is_unique(conn, schema, table_name, [column]):
#             unique_combinations.append([column])
#             break
#
#     # Если нет уникальных отдельных столбцов, проверяем комбинации
#     if not unique_combinations:
#         for i in range(len(sorted_columns)):
#             if unique_combinations:
#                 break
#             for j in range(i + 1, len(sorted_columns)):
#                 if is_unique(conn, schema, table_name, [sorted_columns[i], sorted_columns[j]]):
#                     if unique_combinations:
#                         break
#                     unique_combinations.append([sorted_columns[i], sorted_columns[j]])
#
#
#     return unique_combinations

def find_unique_combinations(conn, schema, table_name, max = None):
    columns = get_columns(conn, schema, table_name)
    unique_value_counts = get_unique_value_counts(conn, schema, table_name, columns)
    sorted_columns = sorted(unique_value_counts, key=unique_value_counts.get, reverse=True)
    unique_combinations = []

    # Проверяем комбинации любой длины от 1 до количества всех столбцов
    for r in range(1, len(sorted_columns) + 1):
        for combination in itertools.combinations(sorted_columns, r):
            if max and len(list(combination))>max:
                return unique_combinations
            if is_unique(conn, schema, table_name, list(combination)):
                unique_combinations.append(list(combination))
                # Если нашли хотя бы одну уникальную комбинацию, прекращаем поиск
                return unique_combinations

    return unique_combinations

def find_unique_combinations_parallel(conn, schema, table_name):
    columns = get_columns(conn, schema, table_name)
    unique_value_counts = get_unique_value_counts(conn, schema, table_name, columns)
    sorted_columns = sorted(unique_value_counts, key=unique_value_counts.get, reverse=True)
    unique_combinations = []

    # Подготовка комбинаций столбцов
    column_combinations = []
    for r in range(1, len(sorted_columns) + 1):
        column_combinations.extend(list(itertools.combinations(sorted_columns, r)))

    # Параметры подключения к базе данных
    dsn_parameters = conn.get_dsn_parameters()
    conn_params = (dsn_parameters.get('host'), dsn_parameters.get('port'), dsn_parameters.get('dbname'), dsn_parameters.get('user'), pss)

    # Создаем пул процессов
    with Pool(processes=5) as pool:
        # Подготавливаем задачи для параллельного выполнения
        tasks = [(conn_params, schema, table_name, list(combination)) for combination in column_combinations]

        # Запускаем задачи параллельно
        results = pool.map(is_unique_task, tasks)

        # Обрабатываем результаты
        for columns, is_unique_result in results:
            if is_unique_result:
                unique_combinations.append(list(columns))
                # Как только нашли первую уникальную комбинацию, останавливаем выполнение
                return unique_combinations

    return unique_combinations


# Подсчет количества строк в таблице
def get_row_count(conn, schema, table_name):
    with conn.cursor() as cur:
        query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table_name)
        )
        cur.execute(query)
        row_count = cur.fetchone()[0]
    return row_count


def conn_db():
    return connect_to_db('10.122.3.134', '5432', 'greenplum-dwh', "gpadmin", pss)

if __name__ == '__main__':
    host = '10.122.3.134'
    port = 22
    username = 'root'  # Подключение по SSH под пользователем root
    db_name = 'greenplum-dwh'

    # Список таблиц для обработки
    table_names = [
        # 'ods__kafka.logger_events',
        # 'datamart_cod.tx_turnover',
        # 'datamart_cod.tx_turnover_per_day',
        # 'datamart.tx_turnover',
        # 'datamart.tx_turnover_per_day',
        # 'datamart_cod.fido_turnover',
        # 'datamart.fido_turnover',
        # 'ods__tranzaxis.avo_doer',
        'ods__tranzaxis.avo_tran',
        # 'datamart.avobank_ma_base',
        # 'ods__wings.process_par_value',
        # 'datamart_cod.entry',
        # 'ods__tranzaxis.avo_entry_part',
        # 'datamart.entry',
        # 'temp.impressions',
        # 'ods__fido.transacts_history',
        # 'datamart.contract_classification_subaccount',
        # 'datamart.contract_actual_tech',
        # 'datamart.contract_classification',
        # 'ods__tranzaxis_cod.avo_entry',
        # 'ods__tranzaxis.avo_entry',
        # 'ods__fido.leads_history',
        # 'ods__tranzaxis_cod.avo_entry_part',
        # 'ods__fido.saldo',
        # 'datamart.contract_classification_inactive_tech',
        # 'ods__appsflyer.inapps',
        # 'ods__asterisk.cdr',
        # 'datamart.dmr_matrix',
        # 'datamart.avobank_ma_monthly',
        'ods__avo_message_storage.messages',
        # 'datamart.portfolio_daily',
        # 'datamart.portfolio_daily_tranzaxis',
        # 'ods__appsflyer.sessions',
        # 'ods__appsflyer.clicks',
        # 'ods__avo_users.histories_users_actions',
        # 'ods__avo_automats.automat_status_history',
        # 'datamart.fido_card_turnover',
        # 'ods__asterisk.call_history',
        # 'ods__avo_operation_manager.transactions',
        # 'ods__avo_attempts.attempt_history',
        # 'ods__asterisk.queue_log',
        # 'datamart.contract_classification_delinquent_tech',
        # 'datamart_cod.accounts',
        # 'ods__tranzaxis_cod.avo_account',
        # 'datamart.accounts',
        # 'ods__tranzaxis.avo_account',
        # 'ods__avo_operation_history.operations',
        # 'ods__kafka.tx_events',
        # 'ods__wings.process',
        # 'datamart.portfolio',
        # 'ods__tranzaxis.avo_creditline_min_pay_delta',
        # 'ods__fido_cod.accounts_history',
        # 'ods__fido.accounts_history',
        # 'ods__fido_cod.accounts',
        # 'ods__fido.accounts',
        # 'ods__fido.accounts_y',
        # 'datamart.contract_classification_transactor_tech',
        # 'ods__tranzaxis.avo_creditline_cycle',
        # 'ods__avo_operation_manager.operations',
        # 'ods__crm.template_answers',
        # 'ods__asterisk.auto_dial_outbound_campaign_data_set',
        # 'datamart.katm_incomes',
        # 'ods__tranzaxis.avo_contract',
        # 'datamart.contracts',
        # 'history__tranzaxis.avo_contract',
        # 'ods__avo_registries.registries_records',
        # 'ods__wings.tasks_res',
        # 'datamart.logger_user_device_map',
        # 'datamart.soft_collection_queue_log',
        # 'ods__avo_attempts.attempt',
        # 'datamart.wings_cl_events',
        # 'datamart.avo_users',
        # 'datamart.avo_app_install',
        # 'ods__appsflyer.installs',
        # 'ods__fido.transacts',
        # 'ods__avo_offers.command',
        # 'ods__crm.service_requests',
        # 'ods__wings.reports',
        # 'ods__avo_device_manager.user_devices',
        # 'ods__wings.reports_request',
        # 'ods__wings.tasks_req',
        # 'ods__avo_users.app_users',
        # 'datamart.avo_users_mapp',
        # 'datamart.katm_no_nesting_data',
        # 'datamart.device_kyc_success_events',
        # 'ods__tranzaxis.avo_card',
        # 'ods__tranzaxis.avo_token',
        # 'ods__wings.cases',
        # 'datamart_risks.application',
        # 'datamart.avo_users_kyc_oper',
        # 'ods__wings.card_history',
        # 'ods__wings.features',
        # 'datamart.avo_users_kyc_daily',
        # 'ods__tranzaxis.avo_creditline_min_pay',
        # 'ods__fido.leads',
        # 'ods__fido.client_current',
        # 'datamart.creditline_contracts',
        # 'ods__tranzaxis.avo_creditline_contract',
        # 'datamart_risks.myid',
        # 'datamart.client_id_links',
        # 'datamart.clients_red',
        # 'datamart.clients',
        # 'ods__appsflyer.uninstalls',
        # 'datamart.avo_clients_offer_oper',
        # 'ods__fido.saldo_y',
        # 'ods__ktc.historical_cash_unit_status_p',
        # 'ods__wings.users_expired_consents',
        # 'ods__fido.dwh_coa',
        # 'ods__avo_internal_cards.card',
        # 'ods__wings.offers',
        # 'ods__avo_offers.offer',
        # 'datamart.avo_clients_offer_daily',
        # 'ods__avo_offers.cl_transaction',
        # 'datamart.automat_status',
        # 'ods__fido.ln_card',
        # 'ods__wings.rwa',
        # 'ods__appsflyer.organic_uninstalls',
        # 'ods__facebook.insights',
        # 'ods__facebook.ads',
        # 'anabasys_sandbox.humo_scorr_aug_to_sep',
        # 'dictionary.sales_reestr',
        # 'ods__odata_1c.accounting_register',
        # 'anabasys_sandbox.humo_scorr_aug',
        # 'ods__minio.pd_model_monitoring',
        # 'datamart.offer_clip',
        # 'ods__wings.temp_users_checked',
        # 'ods__jira.tickets_custex',
        # 'ods__uzcard.uzcard_tran_to_sep',
        # 'ods__vk.statistics',
        # 'ods__uzcard.uzcard_tran_aug',
        # 'ods__appsflyer.clicks_retargeting',
        # 'ods__google_ads.ad_basic_stats',
        # 'ods__appsflyer.inapps_retargeting',
        # 'ods__google_ads.ad_group',
        # 'ods__appsflyer.sessions_retargeting',
        # 'ods__odata_1c.expenses',
        # 'ods__google_ads.campaign',
        # 'ods__facebook.campaigns',
        # 'ods__avo_registries.registries',
        # 'ods__avo_marketplace.services',
        # 'ods__avo.currency_rates',
        # 'datamart.hr_employee_history',
        # 'datamart.tx_fido_oper_day',
        # 'datamart_cod.tx_fido_oper_day',
        # 'ods__fido.day_operational',
        # 'ods__fido_cod.day_operational',
        # 'ods__fido.v_coa',
        # 'ods__asterisk.auto_dial_outbound_campaign',
        # 'ods__wings.r_par',
        # 'ods__avo_marketplace.merchants',
        # 'ods__fido.dwh_coa_y',
        # 'dictionary.balance_code_ru',
        # 'dictionary.io_fraud',
        # 'ods__odata_1c.employee_personal_data',
        'ods__odata_1c.employee_register',
        # 'ods__odata_1c.employees',
        # 'dictionary.calendar',
        # 'ods__fido.dep_accounts',
        # 'ods__odata_1c.accounts',
        # 'ods__tranzaxis_cod.avo_oper_day',
        # 'ods__tranzaxis.avo_oper_day',
        # 'ods__asterisk.agent_queues',
        # 'ods__odata_1c.counterparties',
        # 'ods__asterisk.agents',
        # 'ods__fido.dep_contracts',
        # 'dictionary.avobank_account_mapping',
        # 'datamart.hr_dep_structure',
        # 'dictionary.bank_account_register',
        # 'dictionary.countries',
        # 'ods__vk.banners',
        # 'ods__tranzaxis.avo_account_plan_item',
        # 'ods__odata_1c.expenses_dict',
        # 'ods__avo_automats.automat',
        # 'ods__crm.template_questions',
        # 'dictionary.currencies',
        # 'ods__odata_1c.departments',
        # 'ods__wings.guides',
        # 'dictionary.mobile_standart_notification',
        # 'dictionary.operation_specification',
        # 'ods__tranzaxis.avo_terminal',
        # 'dictionary.countries_offshore',
        # 'dictionary.mobile_tariff',
        # 'ods__ktc.clients',
        # 'dictionary.media_group',
        # 'dictionary.atm_point_info',
        # 'dictionary.dmr_matrix_accounts',
        # 'dictionary.attraction_channel',
        # 'dictionary.operation_scheme',
        # 'ods__tranzaxis.avo_contract_type',
        # 'ods__avo_marketplace.categories',
        # 'dictionary.contract_subaccounts',
        # 'dictionary.fatf_high_risk_list',
        # 'dictionary.offer_status_mapp',
        # 'dictionary.mobile_notification_parts',
        # 'dictionary.operation_purpose',
        # 'dictionary.regions_ru',
        # 'ods__fido.v_region',
        # 'dictionary.mobile_code',
        # 'ods__ktc.cash_unit_statuses',
        # 'dictionary.ltv_marketing_expences_mapping',
        # 'ods__crm.reasons1',
        # 'dictionary.bonus_offers',
        # 'dictionary.offer_status',
        # 'ods__odata_1c.cfo',
        # 'dictionary.mobile_operator',
        # 'ods__tranzaxis.avo_token_life_phase',
        # 'dictionary.contract_class',
        # 'ods__tranzaxis.avo_token_product',
        # 'ods__tranzaxis.avo_card_product',
        # 'ods__odata_1c.currencies',
        # 'dictionary.mobile_notification_type',
        # 'dictionary.automat_ping_interval',
        # 'ods__vk.groups',
    ]

   # table_names = ['ods__fido.leads_history',]

    # Подключаемся к базе данных
    conn = connect_to_db('10.122.3.134', '5432', db_name, "gpadmin", pss)

    # Для каждой таблицы
    def file_tabe():
        for table_name in table_names:
            sch, tab = table_name.split('.')  # Разделение схемы и таблицы
            ddl = get_table_ddl_via_su(host, port, username, db_name, table_name)  # Получение DDL

            if ddl:
                # Имя файла для сохранения
                file_name = f"{table_name.replace('.', '_')}_ddl.sql"
                pth = r'/Users/avotech/Library/DBeaverData/workspace6/General/Scripts'
                file_name = os.path.join(pth,file_name)
                print (file_name)
                unique_combinations = find_unique_combinations(conn, sch, tab)  # Поиск уникальных комбинаций полей
                # Сохранение DDL в файл
                with open(file_name, 'w') as ddl_file:
                    row_count = get_row_count(conn, sch, tab)  # Подсчет строк
                    ddl += f"\nКоличество строк в таблице {sch}.{tab}: {row_count}"

                    if unique_combinations:
                        ddl += f"\nУникальные комбинации полей в таблице {sch}.{tab}: {unique_combinations}"
                    else:
                        ddl += f"\nУникальные комбинации полей не найдены в таблице {sch}.{tab}."

                    ddl_file.write(ddl)  # Запись в файл

                print(f"DDL для таблицы {table_name} сохранен в {file_name}")


    def replace_partition_text(text):
        start_date_match = re.search(r"START \('(.*?)'::date\)", text)
        end_date_match = re.findall(r"END \('(.*?)'::date\)", text)

        if not start_date_match or not end_date_match:
            return text
        first_date = start_date_match.group(1)
        last_date = end_date_match[-1]

        pattern = re.compile(
            r"(DISTRIBUTED (RANDOMLY|BY.*)) PARTITION BY RANGE\(oper_day\)\s*\(.*?DEFAULT PARTITION.*?\);",
            re.DOTALL
        )

        def replacement(match):
            distribution = match.group(1)
            return (
                f"{distribution} PARTITION BY RANGE(oper_day)\n"
                f"(\n"
                f"START ('{first_date}'::date) END ('{last_date}'::date) EVERY ('7 days'::interval),\n"
                f"DEFAULT PARTITION default_partition  \n);"
            )

        return pattern.sub(replacement, text)

    def comment_specified_rows(text):
        pattern = re.compile(
            r"^(SET .*|SELECT pg_catalog\\.set_config\\(.*\\);|REVOKE ALL ON TABLE .*;|GRANT .* ON TABLE .*;|^\s*COLUMN .*;)$",
            re.MULTILINE
        )
        buf = pattern.sub('', text)
        pattern = re.compile(r"^\s*COLUMN\s.*(?:\n|$)", re.MULTILINE)
        buf= pattern.sub('', buf)
        return buf


    def replace_partitions(text: str) -> str:
        pattern = (
            r"START\s+\('\d{4}-\d{2}-\d{2}'::date\)\s+END\s+\('\d{4}-\d{2}-\d{2}'::date\)\s+EVERY\s+\('\d+\s+\w+'::interval\)"
            r"(\s+WITH\s+\(tablename=.*?\))?,?\s*"
        )
        result = re.sub(pattern, "#PARTITION#,\n", text)
        pattern = (
            r"START\s+\('.*?'\s*::timestamp with time zone\)\s+END\s+\('.*?'\s*::timestamp with time zone\)\s+EVERY\s+\('.*?'\s*::interval\),?"
        )
        result = re.sub(pattern, "#PARTITION#,", result)
        pattern = (
            r"START\s+\('\d{4}-\d{2}-\d{2}'::date\)\s+"
            r"END\s+\('\d{4}-\d{2}-\d{2}'::date\)\s*"
            r"(WITH\s+\(.*?\))?,?\s*"
        )
        result = re.sub(pattern, "#PARTITION#,\n", result)
        return result


    class TimeoutException(Exception):
        pass

    def timeout_handler(signum, frame):
        raise TimeoutException()

    def format_sql_with_timeout(ddl, timeout_seconds):
        start_time = time.time()

        try:
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout_seconds)
            formatted_ddl = format_sql_with_sqlfluff(ddl)
        except Exception as e:
            print(f"Error format_sql_with_sqlfluff: {e} {traceback.format_exc()}")
            formatted_ddl = ddl
        finally:
            signal.alarm(0)

        end_time = time.time()
        elapsed_time = end_time - start_time

        print(f"Time taken to format SQL: {elapsed_time:.2f} seconds")

        return formatted_ddl


    def extract_first_and_last_dates(ddl_text: str) -> str:
        start_dates = re.findall(r"START\s+\('([\d-]+)'::date\)", ddl_text)
        end_dates = re.findall(r"END\s+\('([\d-]+)'::date\)", ddl_text)
        if not (start_dates and end_dates):
            start_dates = re.findall(r"START\s+\('([\d-]+\s[\d:]+(?:\+\d{2})?)'::(?:timestamp with time zone|date)\)",
                                     ddl_text)
            end_dates = re.findall(r"END\s+\('([\d-]+\s[\d:]+(?:\+\d{2})?)'::(?:timestamp with time zone|date)\)",
                                   ddl_text)

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
                result = ddl_text.replace('#PARTITION#', first_res)
                pattern = r"WITH\s+\(tablename='[^']+',\s*appendonly='true',\s*orientation='[^']+',\s*compresstype=[^,]+,\s*compresslevel='[1-5]'\s*\)"
                result = re.sub(pattern, '', result)
                pattern = (
                    r"DEFAULT PARTITION default_partition\s+WITH\s+\(tablename=.*?,\s*orientation=.*?,\s*appendonly=.*?,\s*blocksize=.*?,\s*compresstype=.*?,\s*compresslevel=.*?\s*\)"
                )
                result = re.sub(pattern, "DEFAULT PARTITION default_partition", result)
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
        # cleaned_sql = re.sub(r"\s+", " ", cleaned_sql).strip()
        return cleaned_sql


    def format_sql_with_sqlfluff(ddl):
        try:
            linter = Linter(dialect='postgres')
            lint_result = linter.lint_string(ddl, fix=True)

            if lint_result.tree:
                fixed_sql = lint_result.tree.raw
                return fixed_sql
            else:
                return ddl
        except Exception as e:
            print(f'error {e}')
            return ddl


    def format_sql_with_sqlparse(ddl):
        formatted_sql = sqlparse.format(ddl, reindent=True, keyword_case='upper')
        return formatted_sql


    def get_table_ddl_via_su_timeout(host, port, username, db_name, table_name, timeout_seconds):
        start_time = time.time()

        try:
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout_seconds)
            formatted_ddl = get_table_ddl_via_su(host, port, username, db_name, table_name)
        except TimeoutException:
            print(f"error {table_name} Formatting timed out after {timeout_seconds} seconds")
            formatted_ddl = None
        except Exception as e:
            print(f"Error {table_name}: Failed to retrieve DDL for table 'table_name' due to: {e}")
            formatted_ddl = None
        finally:
            signal.alarm(0)

        end_time = time.time()
        elapsed_time = end_time - start_time

        return formatted_ddl


    def modify_number_range(ssql):
        partition_pattern = (
            r"START \('(?P<start>\d+)'::bigint\) END \('(?P<end>\d+)'::bigint\) EVERY \((?P<every>\d+)\).*?WITH \(tablename='[^']*', appendonly='true', orientation='column', compresstype=zstd, compresslevel='(?P<compresslevel>\d+)' \)"
        )
        partitions = re.findall(partition_pattern, ssql)
        if not partitions:
            return ssql

        # Extract the dynamic values for START, END, and EVERY
        start_range = partitions[0][0]
        end_range = partitions[-1][1]
        every_value = partitions[0][2]

        ssql = re.sub(partition_pattern, '', ssql)

        collapsed_partition = (
            f"START ('{start_range}'::bigint) END ('{end_range}'::bigint) EVERY ({every_value})"
        )
        ssql = re.sub(
            r"PARTITION BY RANGE\(r_par_id\)\s*\(",
            f"PARTITION BY RANGE(r_par_id) (\n          {collapsed_partition},",
            ssql,
        )

        # Clean up extra commas and close the partition definition
        ssql = re.sub(r",\s*\)", "\n          )", ssql)

        return ssql

    def one_file():
        file_name = f"res_ddl.sql"
        pth = r'/Users/avotech/Library/DBeaverData/workspace6/General/Scripts'
        file_name = os.path.join(pth, file_name)
        print(file_name)
        for table_name in table_names:
            sch, tab = table_name.split('.')  # Разделение схемы и таблицы
            ddl = get_table_ddl_via_su_timeout(host, port, username, db_name, table_name,40)  # Получение DDL

            if ddl:
                #formatted_ddl = format_sql_with_timeout(ddl, 20)
                formatted_ddl = ddl
                if not formatted_ddl:
                    formatted_ddl = ddl

                # Further beautify using SQLparse
                formatted_ddl = formatted_ddl.replace('PARTITION old ','')
                beautified_ddl = clean_and_format_sql(formatted_ddl)
                beautified_ddl = comment_specified_rows(beautified_ddl)
                pattern = r";\s*;"
                beautified_ddl = re.sub(pattern, ";", beautified_ddl)
                pattern = r"\sENCODING\s*\(.*?\)"
                beautified_ddl = re.sub(pattern, "", beautified_ddl)
                beautified_ddl = clean_and_format_sql(beautified_ddl)
                beautified_ddl = modify_number_range(beautified_ddl)
                start_match = re.search(r'^\s*START\s*\(', beautified_ddl, re.MULTILINE)
                if start_match:
                    start_indent = start_match.group(0).count(' ') - 1
                    beautified_ddl = re.sub(r'^\s*DEFAULT PARTITION', ' ' * start_indent + 'DEFAULT PARTITION',
                                            beautified_ddl,
                                            flags=re.MULTILINE)
                beautified_ddl = replace_partition_text(beautified_ddl)
                beautified_ddl = replace_default_partition(beautified_ddl)
                #unique_combinations = find_unique_combinations(conn, sch, tab)  # Поиск уникальных комбинаций полей
                unique_combinations = None
                # Сохранение DDL в файл
                with open(file_name, 'a') as ddl_file:
                    #row_count = get_row_count(conn, sch, tab)  # Подсчет строк
                    #ddl += f"\n--Количество строк в таблице {sch}.{tab}: {row_count}"

                    beautified_ddl += '\n'
                    ddl_file.write(beautified_ddl)  # Запись в файл

                print(f"DDL для таблицы {table_name} сохранен в {file_name}")


def replace_default_partition(sql):
    pattern = r"(DEFAULT PARTITION\s+[^ ]+\s+).*?(\);)"
    modified_sql = re.sub(pattern, r"\1\2", sql, flags=re.DOTALL)

    return modified_sql

def format_ddl(ddl):
    # formatted_ddl = format_sql_with_timeout(ddl, 10)
    # if not formatted_ddl:
    #     formatted_ddl = ddl
    beautified_ddl = clean_and_format_sql(ddl)
    #beautified_ddl = format_sql_with_sqlfluff(beautified_ddl)
    beautified_ddl = comment_specified_rows(beautified_ddl)
    beautified_ddl = replace_partition_text(beautified_ddl)
    pattern = r";\s*;"
    beautified_ddl = re.sub(pattern, ";", beautified_ddl)
    pattern = r"\sENCODING\s*\(.*?\)"
    beautified_ddl = re.sub(pattern, "", beautified_ddl)
    beautified_ddl.replace(' unknown',' text')
    start_match = re.search(r'^\s*START\s*\(', beautified_ddl, re.MULTILINE)
    if start_match:
         start_indent = start_match.group(0).count(' ') - 1
         beautified_ddl = re.sub(r'^\s*DEFAULT PARTITION', ' ' * start_indent + 'DEFAULT PARTITION',
                                 beautified_ddl,
                                 flags=re.MULTILINE)
    beautified_ddl = replace_default_partition(beautified_ddl)
    return beautified_ddl

def get_ddl_schemas():
    schemas = [
        "datamart", "datamart_cod", "datamart_risks", "dictionary",
        "ods__appsflyer", "ods__asterisk", "ods__avo", "ods__avo_attempts",
        "ods__avo_device_manager", "ods__avo_marketplace", "ods__avo_message_storage",
        "ods__avo_offers", "ods__avo_operation_history", "ods__avo_operation_manager",
        "ods__avo_registries", "ods__avo_users", "ods__crm", "ods__fido", "ods__fido_cod",
        "ods__kafka", "ods__ktc", "ods__odata_1c", "ods__tranzaxis", "ods__tranzaxis_cod",
        "ods__wings", "stg__appsflyer", "temp"
    ]

    if schemas:

        pth = r'/Users/avotech/Library/DBeaverData/workspace6/General/Scripts'
        for schema in schemas:
            file_name = f"res_{schema}_ddl.sql"
            output_file = os.path.join(pth, file_name)
            ddl = get_schema_ddl_via_su(host, port, username, db_name, schema)
            if ddl:
                bddl = format_ddl(ddl)
                with open(output_file, 'w') as f:
                    f.write(bddl)
                print(f"DDL for {schema}")
            else:
                with open(output_file, 'w') as f:
                    f.write(f"-- No DDL retrieved for {schema}\n")
                print(f"No DDL retrieved for {schema}")
            break

one_file()