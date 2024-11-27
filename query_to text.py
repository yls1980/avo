import psycopg2
from psycopg2 import sql
from secr import pss

def connect_to_db(host, port, dbname, user, password):
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    return conn
conn = connect_to_db('10.122.3.134', '5432', 'greenplum-dwh', "gpadmin", pss)

def get_data():
    with conn.cursor() as cur:
        query = sql.SQL("""
            select distinct query from temp.query_app_users;
        """)
        cur.execute(query)
        data = cur.fetchall()
        with open('/Users/avotech/app_users_queries.sql', 'w') as sql_file:
            for query in data:
                sql_file.writelines(query[0])

get_data()