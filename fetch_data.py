import json

import requests

required_cols = ['id', 'name', 'tagline', 'first_brewed', 'abv']

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


if __name__ == '__main__':
    api_end_point = 'beers'
    data = fetch_api_data(api_end_point, 1, 10)
    save_json_to_file(data, 'json_data')
