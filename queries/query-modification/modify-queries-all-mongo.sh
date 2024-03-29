#!/bin/bash
QUERIES_DIR="/home/user/ntua_adis/queries/original-queries"
OUT_DIR="/home/user/ntua_adis/queries/modified-queries"

MONGO_TABLES=("web_sales" "catalog_sales" "catalog_page" "catalog_returns" "call_center" "warehouse" "date_dim" "time_dim" "web_returns" "web_site" "web_page" "store_sales" "store_returns" "store" "inventory" "ship_mode" "promotion" "item" "reason" "customer" "customer_address" "customer_demographics" "household_demographics" "income_band")
REDIS_TABLES=()
CASSANDRA_TABLES=()

declare -A TABLE_MAPPING

for table in "${MONGO_TABLES[@]}"; do
    TABLE_MAPPING["$table"]=mongodb.tpcds.$table
done

for table in "${REDIS_TABLES[@]}"; do
    TABLE_MAPPING["$table"]=redis.$table.$table
done

for table in "${CASSANDRA_TABLES[@]}"; do
    TABLE_MAPPING["$table"]=cassandra.tpcds.$table
done

mkdir -p "$OUT_DIR"
rm -r "$OUT_DIR/*"
for sql_file in "$QUERIES_DIR"/*.sql; do
    echo $sql_file
    OUT_FILE="$OUT_DIR/$(basename "$sql_file")"
    echo $OUT_FILE
    # Copy the original file to a new file in the modified directory
    cp "$sql_file" "$OUT_FILE"

    for table in "${!TABLE_MAPPING[@]}"; do
        TABLE_OLD="$table"
        TABLE_NEW="${TABLE_MAPPING[$table]}"

        echo "$TABLE_OLD -> $TABLE_NEW"
        # sed -i '' "s/$TABLE_OLD/$TABLE_NEW/g" $OUT_FILE
        sed -i "s/\<$TABLE_OLD\>/$TABLE_NEW/g" $OUT_FILE
    done
done
