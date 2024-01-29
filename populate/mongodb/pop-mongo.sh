# MongoDB connection parameters
MONGO_HOST="localhost"
MONGO_PORT=27017
MONGO_DB_NAME="tpcds"
table_name="$1"
file_path="$2"
columns="$3"

python3 /home/user/ntua_adis/populate/mongodb/format-mongo.py $file_path

mongoimport --host "$MONGO_HOST" --port "$MONGO_PORT" --db "$MONGO_DB_NAME" --collection "$table_name" --type csv --fields "$columns" --file "$file_path.csv" --numInsertionWorkers 3 --ignoreBlanks
