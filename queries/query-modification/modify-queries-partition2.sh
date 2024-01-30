#!/bin/bash
QUERIES_DIR="/home/user/ntua_adis/queries/original-queries"
OUT_DIR="/home/user/ntua_adis/queries/modified-queries"

MONGO_TABLES=("web_sales" "store_sales" "catalog_sales" "time_dim" "customer_demographics" "inventory")
REDIS_TABLES=("warehouse" "store" "promotion" "item" "reason" "customer" "call_center" "web_page" "web_site" "ship_mode" "income_band")
CASSANDRA_TABLES=("store_returns" "web_returns" "customer_address" "household_demographics" "catalog_page" "catalog_returns" "date_dim")

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
