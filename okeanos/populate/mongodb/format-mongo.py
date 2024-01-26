import csv
import sys
def process_and_save_csv(input_file_path, output_file_path):
    with open(input_file_path, 'r', encoding='latin1') as infile, \
         open(output_file_path, 'w', encoding='latin1', newline='') as outfile:
        # Create a CSV reader
        csv_reader = csv.reader(infile, delimiter='|')

        # Create a CSV writer for the output file
        csv_writer = csv.writer(outfile)

        # Write the header to the output file
        header = next(csv_reader)
        csv_writer.writerow(header)

        # Process and save each row
        for row in csv_reader:
            # Replace | with commas and wrap fields containing commas with double quotes
            row = [f'{field}' if ',' in field else field for field in row]

            # Write the modified row to the output file
            csv_writer.writerow(row)

# Example usage
input_file_path = sys.argv[1]
output_file_path = f'{input_file_path}.csv'

process_and_save_csv(input_file_path, output_file_path)
