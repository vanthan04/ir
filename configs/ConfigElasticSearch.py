import json
import pickle
import os
import time
from elasticsearch import Elasticsearch, helpers
from httpx import TransportError

# Lấy đường dẫn tuyệt đối tới file hiện tại (ConfigElasticSearch.py)
current_dir = os.path.dirname(os.path.abspath(__file__))

# Tạo đường dẫn tới file config.json
config_path = os.path.join(current_dir, 'config.json')

with open(config_path) as config_file:
    config = json.load(config_file)


def connect_elasticsearch():
    client = Elasticsearch(
        hosts=config['elasticsearch']['hosts'],
        api_key=config['elasticsearch']['API_KEY'],
        timeout=60,  # Tăng thời gian chờ (timeout) lên 60 giây
        max_retries=10,  # Tăng số lần thử lại
        retry_on_timeout=True  # Bật retry nếu timeout
    )
    index_name = config['elasticsearch']['index_name']

    mappings = {
        "properties": {
            "id": {
                "type": "text"
            },
            "title": {
                "type": "text",

            },
            "clean_text": {
                "type": "text"
            }
        }
    }

    mapping_response = client.indices.put_mapping(index=index_name, body=mappings)
    print(mapping_response)
    return client


# Hàm đọc file .pkl
def load_data(file_path):
    with open(file_path, 'rb') as file:
        data = pickle.load(file)
    return data


def process_dict(dict):
    # Chuẩn bị tài liệu cho Elasticsearch bulk API
    docs = [
        {

            "clean_text": dict['clean_text'][i] if 'clean_text' in dict else '',
            "id": dict['id'][i] if 'id' in dict else '',
            "title": dict['title'][i] if 'title' in dict else ''
        }
        for i in range(len(dict['id']))  # Duyệt qua từng hàng trong dict
    ]

    return docs


def bulk_insert_with_retry(client, docs_batch, index_name=config['elasticsearch']['index_name'], max_retries=5):
    retry_count = 0
    while retry_count < max_retries:
        try:
            success, failed = helpers.bulk(client, docs_batch, index=index_name, request_timeout=60)
            print(f"Successfully inserted {success} documents, Failed {failed} documents.")
            break
        except (ConnectionError, TransportError) as e:
            retry_count += 1
            print(f"Error occurred: {e}. Retrying {retry_count}/{max_retries}...")
            time.sleep(2)  # Chờ 2 giây trước khi retry
    if retry_count == max_retries:
        print("Max retries reached. Bulk insert failed for this batch.")



