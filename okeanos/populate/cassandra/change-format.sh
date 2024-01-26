#!/bin/bash

# Specify the directory containing .dat files
dat_directory="/home/user/data"

# Iterate over .dat files in the directory
for filename in "$dat_directory"/*.dat; do
    # Check if the file exists
    if [ -e "$filename" ]; then
        echo "Conveting to UTF-8"
        iconv -f ISO-8859-1 -t UTF-8 "$filename" -o "${filename}.csv"
        echo "removing old file"
        rm "$filename"
    else
        echo "File not found: $filename"
    fi
done

for filename in "$dat_directory"/*.dat.csv; do
    # Check if the file exists
    if [ -e "$filename" ]; then
        # Remove the .csv extension from each file
        new_filename="${filename%.csv}"
        mv "$filename" "$new_filename"

        echo "Renamed $filename to $new_filename"
    else
        echo "File not found: $filename"
    fi
done
