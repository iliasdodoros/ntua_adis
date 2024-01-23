import redis
import pandas as pd

# Function to load a limited number of TPC-DS data into Redis
def load_tpcds_data_into_redis(redis_client, table_name, tpcds_data_path, limit=1000):
    # Load limited TPC-DS data from CSV file into a Pandas DataFrame
    df = pd.read_csv(tpcds_data_path, sep='|', index_col=False, header=None, names=tpcds_columns[table_name], nrows=limit, encoding='latin1')
    # Convert DataFrame to a dictionary where keys are record IDs
    data_dict = df.to_dict(orient='index')
    # Load data into Redis
    load_data_into_redis(redis_client, table_name, data_dict)

# Function to load data into Redis
def load_data_into_redis(redis_client, table_name, data):
    for record_id, record_data in data.items():
        key = f"{table_name}:{record_id}"
        # Use hmset to set multiple fields for each key
        redis_client.hmset(key, record_data)
        print(f"Added {record_id}")

# Example TPC-DS data file paths (replace these with your actual paths)
tpcds_data_paths = {
    'customer': '/home/user/data/customer.dat'    # Add more table names and file paths as needed
}

# Example TPC-DS column names for each table (replace these with your actual column names)
tpcds_columns = {
    'customer': ['c_customer_sk', 'c_customer_id', 'c_current_cdemo_sk', 'c_current_hdemo_sk', 'c_current_addr_sk', 'c_first_shipto_date_sk', 'c_first_sales_date_sk', 'c_salutation', 'c_first_name', 'c_last_name', 'c_preferred_cust_flag', 'c_birth_day', 'c_birth_month', 'c_birth_year', 'c_birth_country', 'c_login', 'c_email_address', 'c_last_review_date']    # Add more table names and column names as needed
}

# Connect to Redis
redis_client = redis.StrictRedis(host='192.168.2.41', port=6379, decode_responses=True)

# Load a limited number of TPC-DS data into Redis for each table
for table_name, data_path in tpcds_data_paths.items():
    load_tpcds_data_into_redis(redis_client, table_name, data_path, limit=1000)
