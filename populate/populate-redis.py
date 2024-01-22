import redis
import pandas as pd
import json
from redis.commands.json.path import Path

# Function to load TPC-DS data into Redis as separate JSON objects for each row
def load_tpcds_data_into_redis(redis_client, table_name, tpcds_data_path, limit=None):
    # Load TPC-DS data from CSV file into a Pandas DataFrame
    df = pd.read_csv(tpcds_data_path, sep='|', header=None, names=tpcds_columns[table_name], nrows=limit, encoding='latin1')

    # Iterate over each row and store it as a separate JSON in Redis
    for index, row in df.iterrows():
        json_data = row.to_json()
        redis_key = f"{table_name}:{index + 1}"  # Assuming index starts from 0
        redis_client.json().set(redis_key, Path.rootPath(), json_data)
        print(f"Added data to {redis_key}")

# Example TPC-DS data file paths (replace these with your actual paths)
tpcds_data_paths = {
    'customer': '/home/user/data/customer.dat'    # Add more table names and file paths as needed
}

# Example TPC-DS column names for each table (replace these with your actual column names)
tpcds_columns = {
    'customer': ['c_customer_sk', 'c_customer_id', 'c_current_cdemo_sk', 'c_current_hdemo_sk', 'c_current_addr_sk', 'c_first_shipto_date_sk', 'c_first_sales_date_sk', 'c_salutation', 'c_first_name', 'c_last_name', 'c_preferred_cust_flag', 'c_birth_day', 'c_birth_month', 'c_birth_year', 'c_birth_country', 'c_login', 'c_email_address', 'c_last_review_date']    # Add more table names and column names as needed
}

# Connect to Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)

# Load TPC-DS data into Redis for 'customer' table as separate JSONs
for table_name, data_path in tpcds_data_paths.items():
    load_tpcds_data_into_redis(redis_client, table_name, data_path, limit=1000)
