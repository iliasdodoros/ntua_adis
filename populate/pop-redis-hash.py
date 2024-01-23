import redis
import csv

# Function to load a limited number of TPC-DS data into Redis
def load_tpcds_data_into_redis(redis_client, table_name, tpcds_data_path, limit=1000):
    # Open the TPC-DS data file
    with open(tpcds_data_path, 'r', encoding='latin1') as csvfile:
        # Create a CSV reader
        header = tpcds_columns[table_name]
        csv_reader = csv.DictReader(csvfile, fieldnames= header, delimiter='|')

        # Read the header from the TPC-DS column names you provided
        

        # Load a limited number of TPC-DS data into Redis
        for i, row in enumerate(csv_reader):
            if i >= limit:
                break
            print(row)
            del row[None]
            # Create a key for the Redis hash (using the first column)
            key = f"{table_name}:{i+1}"

            # Include all columns in the Redis hash except the key column
           
            # Store the hash data in Redis
            redis_client.hset(key, mapping=row)
            print(f"Added {row[0]}")

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
