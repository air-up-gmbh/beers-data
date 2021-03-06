import json
import re
from datetime import datetime

import pandas as pd
import requests

import util

required_cols = ['id', 'name', 'tagline', 'first_brewed', 'abv', 'ingredients.yeast']

# https://punkapi.com/documentation/v2

api_base_url = 'https://api.punkapi.com/v2'

max_page_size = 80


def fetch_api_data(end_point, page, size):
    try:
        response = requests.get('{base_url}/{end_point}?page={page}&per_page={size}'
                                .format(base_url=api_base_url, end_point=end_point, page=page, size=size))
        return json.loads(response.text)
    except Exception as e:
        print(e)


def save_json_to_file(json_data, file_name):
    with open('data/{}.json'.format(file_name), 'w') as f:
        json.dump(json_data, f)


def convert_json_to_df(json_data, req_cols=None):
    df = pd.json_normalize(json_data)
    if req_cols and len(required_cols) > 0:
        return df[req_cols]
    return df


def save_df_to_sql(df, table_name, method=None):
    # print("Saving data in db for table", table_name)
    df.to_sql(
        name=table_name,
        con=util.get_sql_conn(),
        if_exists='append',
        index=False,
        method=method
    )


def ingest_data(page_size, table_name):
    inc_column = 'id'
    prev_ingested_id = util.get_previous_ingested_value(table_name)
    print('prev_ingested_id', prev_ingested_id)
    page_no = int(prev_ingested_id) // page_size + 1 if prev_ingested_id else 1
    print('page_no', page_no)
    start_time = datetime.now()
    data = fetch_api_data('beers', page_no, page_size)
    df_final = convert_json_to_df(data, required_cols)
    df_final = df_final.rename(columns={"ingredients.yeast": "yeast"})
    save_df_to_sql(df_final, table_name, util.insert_on_duplicate)
    for i, row in df_final.iterrows():
        save_ingredients_hops(i, row['id'], data)
        save_ingredients_malt(i, row['id'], data)
        save_food_pairing(i, row['id'], data)
    util.ingestion_entry(table_name=table_name, start_time=start_time, count=len(df_final),
                         inc_state=df_final[inc_column].max(),
                         inc_column=inc_column, database=util.get_config('database'), status=True)


def fetch_min_and_max_abv_value(table_name):
    abv_min, abv_max = 0, 0
    query = "SELECT MIN(abv), MAX(abv) from {}".format(table_name)
    data = util.execute_query(query, util.get_postgres_conn())
    if data and data[0][0]:
        abv_min, abv_max = data[0][0], data[0][1]
    return abv_min, abv_max


def save_ingredients_hops(index, beer_id, data):
    df_hops = pd.json_normalize(data[index]['ingredients']['hops'])
    df_hops['beer_id'] = beer_id
    df_hops = df_hops.rename(columns={"amount.value": "amount_value", "amount.unit": "amount_unit"})
    save_df_to_sql(df_hops, 'ingredients_hops')


def save_ingredients_malt(index, beer_id, data):
    df_malt = pd.json_normalize(data[index]['ingredients']['malt'])
    df_malt['beer_id'] = beer_id
    df_malt = df_malt.rename(columns={"amount.value": "amount_value", "amount.unit": "amount_unit"})
    save_df_to_sql(df_malt, 'ingredients_malt')


def save_food_pairing(index, beer_id, data):
    foods = data[index]['food_pairing']
    foods = [re.sub('[^a-zA-Z0-9 ]+', '', f) for f in foods]
    df_fp = pd.DataFrame(foods, columns=['name'])
    save_df_to_sql(df_fp, 'food_pairing', util.insert_food_paring)
    save_beer_food_pairing_mapping(foods, beer_id)


def save_beer_food_pairing_mapping(foods, beer_id):
    conn = util.get_postgres_conn()
    food_ids = []
    for food in foods:
        result = util.execute_query("SELECT id from food_pairing where name = '{}'".format(food), conn, False)
        if result and result[0][0]:
            food_ids.append(result[0][0])

    if food_ids:
        df = pd.DataFrame(food_ids, columns=['food_pairing_id'])
        df['beer_id'] = beer_id
        save_df_to_sql(df, 'beer_food_pairing_mapping', util.insert_beer_food_paring)
    conn.close()


if __name__ == '__main__':
    tb_name = 'beer'
    ingest_data(page_size=25, table_name=tb_name)
    min_abv, max_abv = fetch_min_and_max_abv_value(tb_name)
    print(min_abv, max_abv)

# """    insert_query = "INSERT INTO beer_food_pairing_mapping (beer_id, food_pairing_id) VALUES "
#     values = []
#     conn = util.get_postgres_conn()
#     for food in foods:
#         food_id_query = "SELECT id from food_pairing where name = '{}'".format(food)
#         result = util.execute_query(food_id_query, conn, False)
#         if result and result[0][0]:
#             food_id = result[0][0]
#             print(food_id)
#             values.append('({beer_id}, {food_id})'.format(beer_id=beer_id, food_id=food_id))
#     if values:
#         insert_query += ', '.join(values)
#         util.execute_insert_query(insert_query, conn)"""
