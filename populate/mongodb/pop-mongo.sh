# MongoDB connection parameters
MONGO_HOST="localhost"
MONGO_PORT=27017
MONGO_DB_NAME="tpcds"
table_name="$1"
file_path="$2"
declare -A MONGO_TABLES=(
    ["customer"]="c_customer_sk,c_customer_id,c_current_cdemo_sk,c_current_hdemo_sk,c_current_addr_sk,c_first_shipto_date_sk,c_first_sales_date_sk,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address,c_last_review_date"
    # Add other tables as needed
)


mongoimport --host "$MONGO_HOST" --port "$MONGO_PORT" --db "$MONGO_DB_NAME" --collection "$TABLE_NAME" --type csv --fields "${MONGO_TABLES[$table_name]}" --file "$file_path" --ignoreBlanks
