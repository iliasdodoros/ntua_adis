import redis
import csv

# Function to load a limited number of TPC-DS data into Redis
def load_tpcds_data_into_redis(redis_client, table_name, tpcds_data_path, limit=1000):
    # Open the TPC-DS data file
    with open(tpcds_data_path, 'r', encoding='latin1') as csvfile:
        # Create a CSV reader
        csv_reader = csv.reader(csvfile, delimiter='|')

        # Read the header to get column names
        header = next(csv_reader)

        # Load a limited number of TPC-DS data into Redis
        for i, row in enumerate(csv_reader):
            if i >= limit:
                break

            # Create a key for the Redis hash (using the first column)
            key = f"{table_name}:{row[0]}"

            # Convert record_data values to strings before storing in Redis
            string_row = {str(header[j]): str(value) for j, value in enumerate(row)}

            # Include all columns in the Redis hash except the key column
            hash_data = {str(header[j]): str(value) for j, value in enumerate(row)}

            # Store the hash data in Redis
            redis_client.hset(key, mapping=hash_data)
            print(f"Added {row[0]}")

# Example TPC-DS data file paths (replace these with your actual paths)
tpcds_data_paths = {
    'customer': '/home/user/data/customer.dat'    # Add more table names and file paths as needed
}

# Connect to Redis
redis_client = redis.StrictRedis(host='192.168.2.41', port=6379, decode_responses=True)

# Load a limited number of TPC-DS data into Redis for each table
for table_name, data_path in tpcds_data_paths.items():
    load_tpcds_data_into_redis(redis_client, table_name, data_path, limit=1000)
