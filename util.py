import configparser
from datetime import datetime

import psycopg2
from sqlalchemy import create_engine, types as sql_types
from sqlalchemy.dialects.postgresql import insert

config_parser = configparser.ConfigParser()
config_parser.read('configs/config.param')

# params_map = {}

data_type = {'id': sql_types.Integer, 'name': sql_types.String(100), 'tagline': sql_types.String(150),
             'first_brewed': sql_types.String(10), 'abv': sql_types.Float}

METADATA_DB = 'METADATA_DB'


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


def get_metadata_conn():
    return psycopg2.connect(
        host=get_config("host", METADATA_DB),
        user=get_config("username", METADATA_DB),
        database=get_config('database', METADATA_DB),
        password=get_config("password", METADATA_DB)
    )


def execute_query(query, conn, close_conn=True):
    cursor = conn.cursor()
    print("Executing query : \n {}".format(query))
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    if close_conn:
        conn.close()
    return result


def execute_insert_query(query, conn, close_conn=True):
    cursor = conn.cursor()
    print("Executing query : \n {}".format(query))
    cursor.execute(query)
    _id = cursor.lastrowid
    conn.commit()
    if close_conn:
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


def insert_food_paring(table, conn, keys, data_iter):
    insert_stmt = insert(table.table).values(list(data_iter))
    do_nothing_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['name'])
    conn.execute(do_nothing_stmt)


def insert_beer_food_paring(table, conn, keys, data_iter):
    insert_stmt = insert(table.table).values(list(data_iter))
    do_nothing_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['beer_id', 'food_pairing_id'])
    conn.execute(do_nothing_stmt)


def ingestion_entry(table_name, start_time, count, inc_state, inc_column, database, status,
                    load_type='INC', freq='daily'):
    """Insert ingestion metadata table entry
       Args:
           table_name (str): Name of Google API
           start_time (datetime): Ingestion start time
           count (int): Total record count
           inc_column (str): Table increment column
           inc_state (str): Increment column value
           database (str): Table database name
           status (bool): Data ingestion status
           load_type (str): INC/FULL default 'INC'
           freq (str): Data ingestion frequency default 'daily'
       Returns:
           None
       """
    try:
        query = """
        INSERT INTO {ingestion_tb} 
        (db_name, table_name, frequency, inc_col, no_of_record, inc_state, load_type, 
        load_timestamp, total_exe_time_sec, state_of_run) 
        VALUES ('{db}', '{table_name}','{freq}','{inc_column}','{count}', '{inc_state}',
        '{load_type}','{load_timestamp}', '{exe_time}','{status}')""" \
            .format(ingestion_tb=get_config('ingestion_table', METADATA_DB),
                    db=database, table_name=table_name, freq=freq, inc_column=inc_column,
                    count=count, inc_state=inc_state, load_type=load_type,
                    load_timestamp=str(datetime.now()),
                    exe_time=round((datetime.now() - start_time).total_seconds()), status=status)
        execute_insert_query(query, get_metadata_conn())
    except Exception as e:
        raise Exception("\nIngestion Entry  failed for " + table_name, e)


def get_previous_ingestion_date(table_name):
    result = 0
    query = "select max(NULLIF(inc_state, '')::INT) from {ingestion_table} where table_name = '{tb}' and state_of_run=True" \
        .format(ingestion_table=get_config('ingestion_table', METADATA_DB), tb=table_name)
    print("previous ingestion details fetch query : \n {}".format(query))
    query_data = execute_query(query, get_metadata_conn())
    if query_data[0][0] is not None:
        result = query_data[0][0]
        print('Prev {} Ingestion details - {}'.format(table_name, query_data[0][0]))
        return result
    return result


"""



delete from beer_food_pairing_mapping;
delete from food_pairing;
delete from ingredients_malt;
delete from ingredients_hops;
delete from beer;

delete from ingestion_history ;
"""
