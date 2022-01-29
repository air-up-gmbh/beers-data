import configparser

import psycopg2
from sqlalchemy import create_engine, types as sql_types
from sqlalchemy.dialects.postgresql import insert

config_parser = configparser.ConfigParser()
config_parser.read('configs/config.param')

# params_map = {}

data_type = {'id': sql_types.Integer, 'name': sql_types.String(100), 'tagline': sql_types.String(150),
             'first_brewed': sql_types.String(10), 'abv': sql_types.Float}


def get_config(key, tag='DB'):
    # if params_map.get(tag + key) is not None:
    #     return params_map.get(tag + key)
    return config_parser[tag][key].strip()


def get_postgres_conn():
    return psycopg2.connect(
        host=get_config("host"),
        user=get_config("username"),
        database=get_config('database'),
        password=get_config("password")
    )


def execute_query(query):
    conn = get_postgres_conn()
    cursor = conn.cursor()
    print("Executing query : \n {}".format(query))
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result


def execute_insert_query(query):
    conn = get_postgres_conn()
    cursor = conn.cursor()
    print("Executing query : \n {}".format(query))
    cursor.execute(query)
    _id = cursor.lastrowid
    conn.commit()
    cursor.close()
    conn.close()


def get_sql_conn():
    return create_engine('postgresql://{un}:{ps}@{host}:5432/{db}'
                         .format(host=get_config("host"),
                                 un=get_config("username"),
                                 db=get_config("database"),
                                 ps=get_config("password")))


# To avoid
def insert_on_duplicate(table, conn, keys, data_iter):
    insert_stmt = insert(table.table).values(list(data_iter))
    do_nothing_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['id'])
    conn.execute(do_nothing_stmt)
