import csv
import redis

# Function to load a limited number of TPC-DS data into Redis
def load_tpcds_data_into_redis(redis_client, table_name, tpcds_data_path, limit=1000):
    with open(tpcds_data_path, 'r', encoding='latin1') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='|')
        
        # Skip header if it exists
        next(csv_reader, None)
        
        # Load a limited number of TPC-DS data from CSV file
        for row_count, row in enumerate(csv_reader):
            if row_count >= limit:
                break
            
            # Extract column names and values
            column_names = tpcds_columns[table_name]
            record_data = dict(zip(column_names, row))
            
            # Load data into Redis
            load_data_into_redis(redis_client, table_name, row_count, record_data)

# Function to load data into Redis
def load_data_into_redis(redis_client, table_name, record_id, record_data):
    key = f"{table_name}:{record_id}"
    
    # Include all columns in the Redis hash
    hash_data = {str(k): str(v) for k, v in record_data.items()}
    
    # Remove the key used for the Redis hash if it exists
    hash_data.pop(tpcds_columns[table_name][0], None)
    
    # Store the hash data in Redis
    redis_client.hset(key, mapping=hash_data)
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
