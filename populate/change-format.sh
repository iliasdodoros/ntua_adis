#!/bin/bash

# Specify the directory containing .dat files
dat_directory="/home/user/data"

# Iterate over .dat files in the directory
for filename in "$dat_directory"/*.dat; do
    # Check if the file exists
    if [ -e "$filename" ]; then
        # Remove trailing "|" from each line in the file
        echo "Removing | from ${filename}"
        sed 's/|$//' "$filename" > "${filename}.temp"
        mv "${filename}.temp" "$filename"

        echo "Trailing '|' characters removed from $filename"
        echo "convreting"
        iconv -f ISO-8859-1 -t UTF-8 "$filename" -o "${filename}.csv"
        echo "removing old file"
        rm "$filename"
    else
        echo "File not found: $filename"
    fi
done
