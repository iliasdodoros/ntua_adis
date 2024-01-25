#!/bin/bash

# MongoDB connection parameters
MONGO_HOST="localhost"
MONGO_PORT=27017
MONGO_DB_NAME="tpcds"

# Directory containing .dat files
DAT_FILES_DIR="/home/user/data"

# Define tables for MongoDB and Redis with their corresponding column names
declare -A MONGO_TABLES=(
    ["customer"]="c_customer_sk,c_customer_id,c_current_cdemo_sk,c_current_hdemo_sk,c_current_addr_sk,c_first_shipto_date_sk,c_first_sales_date_sk,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address,c_last_review_date"
    # Add other tables as needed
)

declare -A REDIS_TABLES=(
    ["warehouse"]="sr_returned_date_sk,sr_return_time_sk,sr_item_sk,sr_customer_sk,sr_cdemo_sk,sr_hdemo_sk,sr_addr_sk,sr_store_sk,sr_reason_sk,sr_ticket_number,sr_return_quantity,sr_return_amt,sr_return_tax,sr_return_amt_inc_tax,sr_fee,sr_return_ship_cost,sr_refunded_cash,sr_reversed_charge,sr_store_credit,sr_net_loss"
    # Add other tables as needed
)

# Function to import data to MongoDB
import_to_mongo() {
    local table_name="$1"
    local file_path="$2"
    local columns="${MONGO_TABLES[$table_name]}"
    local collection_key="$table_name"

    echo "Importing $file_path to MongoDB"

    # Use mongoimport to import the data to MongoDB
    mongoimport --host "$MONGO_HOST" --port "$MONGO_PORT" --db "$MONGO_DB_NAME" --collection "$collection_key" --type csv --fields "$columns" --file "$file_path"

    echo "Import complete for $file_path in MongoDB"
}

# Function to import data to Redis
import_to_redis() {
    local table_name="$1"
    echo "Iporting $tablename to Redis"
    # Use redis-cli to load data into Redis hash
    python3 /home/user/ntua_adis/populate/redis/pop-redis-hash.py "$table_name"

    echo "Import complete for $file_path in Redis"
}

# Iterate over each .dat file in the directory
for dat_file in "$DAT_FILES_DIR"/*.dat; do
    # Extract table name from the file name
    table_name=$(basename "$dat_file" .dat)

    # Check if the table should go to MongoDB or Redis
    if [ -n "${MONGO_TABLES[$table_name]}" ]; then
        import_to_mongo "$table_name" "$dat_file"
    elif [ -n "${REDIS_TABLES[$table_name]}" ]; then
        import_to_redis "$table_name"
    else
        echo "Table $table_name not specified for import."
    fi
done

echo "Data import complete."
