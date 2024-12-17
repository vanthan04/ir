# main.py

from configs.ConfigElasticSearch import connect_elasticsearch, load_data, process_dict, bulk_insert_with_retry

# Đường dẫn tới file pickle
file_path = 'wiki_data.pkl'

# Kết nối Elasticsearch
client = connect_elasticsearch()

# Đọc dữ liệu từ file .pkl
data_dict = load_data(file_path)
print("Đọc dữ liệu xong")
# Chuyển đổi dữ liệu sang định dạng cho Elasticsearch
docs = process_dict(data_dict)
print("Chuyển định dạng xong")

# Chèn dữ liệu vào Elasticsearch theo batch
batch_size = 500
for i in range(0, len(docs), batch_size):
    batch = docs[i:i+batch_size]
    print(i)
    bulk_insert_with_retry(client, batch)
