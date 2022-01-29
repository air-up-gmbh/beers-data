import json

import pandas as pd
import requests
import util as ut

required_cols = ['id', 'name', 'tagline', 'first_brewed', 'abv']

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


def save_df_to_sql(df, tb):
    print("Saving data in db for table", tb)
    df.to_sql(
        name=tb,
        con=ut.get_sql_conn(),
        if_exists='append',
        index=False,
        method=ut.insert_on_duplicate
    )


if __name__ == '__main__':
    api_end_point = 'beers'
    table_name = 'beers'
    data = fetch_api_data(api_end_point, 1, 25)
    df_final = convert_json_to_df(data, required_cols)
    save_df_to_sql(df_final, table_name)
