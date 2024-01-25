#!/bin/bash
declare -A TABLE_COLUMNS=(
    ["customer"]="c_customer_sk,c_customer_id,c_current_cdemo_sk,c_current_hdemo_sk,c_current_addr_sk,c_first_shipto_date_sk,c_first_sales_date_sk,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address,c_last_review_date"
)

# Directory containing .dat files
DAT_FILES_DIR="/home/user/data"

MONGO_TABLES=("customer" )
REDIS_TABLES=("warehouse" )
CASSANDRA_TABLES=("web_sales")

# Function to import data to MongoDB
import_to_mongo() {
    local table_name="$1"
    local file_path="$2"

    echo "Importing $file_path to MongoDB"
    # Use mongoimport to import the data to MongoDB
    bash mongodb/pop-mongo.sh "$table_name" "$file_path"

    echo "Import complete for $file_path in MongoDB"
}

# Function to import data to Redis
import_to_redis() {
    local table_name="$1"
    
    echo "Importing $tablename to Redis"
    # Use redis-cli to load data into Redis hash
    python3 /home/user/ntua_adis/populate/redis/pop-redis-hash.py "$table_name"

    echo "Import complete for $file_path in Redis"
}

# Function to generate CQL COPY command for Cassandra
import_to_cassandra() {
    local table_name="$1"
    local columns="${TABLE_COLUMNS[$table_name]}"
    local file_path="$2"
    scp 
    ssh user@192.168.2.40 'cqlsh -e "COPY tpcds.$table_name ($columns) FROM '$file_path';"' 

    echo "CQL COPY command generated for $file_path in Cassandra"
}

# Iterate over each .dat file in the directory
for dat_file in "$DAT_FILES_DIR"/*.dat; do
    # Extract table name from the file name
    table_name=$(basename "$dat_file" .dat)
    echo "$dat_file"
    # # Check if the table should go to MongoDB or Redis
    # if [[ " ${MONGO_TABLES[@]} " =~ " ${table_name} " ]]; then
    #     import_to_mongo "$table_name" "$dat_file"
    # elif [[ " ${REDIS_TABLES[@]} " =~ " ${table_name} " ]]; then
    #     import_to_redis "$table_name"
    # elif [[ " ${CASSANDRA_TABLES[@]} " =~ " ${table_name} " ]]; then
    #     import_to_cassandra "$table_name" "$dat_file"
    # else
    #     echo "Table $table_name not specified for import."
    # fi
done

echo "Data import complete."
