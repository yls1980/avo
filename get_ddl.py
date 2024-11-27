import itertools

import paramiko
import psycopg2
from psycopg2 import sql
from secr import pss  

import multiprocessing
from multiprocessing import Pool


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
        #'ods__avo_attempts.attempt_history',
        #'ods__avo_attempts.attempt',
        #'ods__avo_users.histories_users_actions',
        'ods__kafka.logger_events',
        'ds__kafka.logger_events_hist',
        #'ods__avo_message_storage.messages',
        #'ods__avo_operation_history.operations',
        #'ods__avo_operation_manager.operations',
        #'ods__avo_registries.registries_records',
        #'ods__avo_operation_manager.transactions'
        #'ods__wings.process_par_value'
    ]

    # Подключаемся к базе данных
    conn = connect_to_db('10.122.3.134', '5432', db_name, "gpadmin", pss)

    # Для каждой таблицы
    for table_name in table_names:
        sch, tab = table_name.split('.')  # Разделение схемы и таблицы
        ddl = get_table_ddl_via_su(host, port, username, db_name, table_name)  # Получение DDL

        if ddl:
            # Имя файла для сохранения
            file_name = f"{table_name.replace('.', '_')}_ddl.sql"
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
