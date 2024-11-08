import csv
import re

# Sample input text with multiple rows
with open('/Users/avotech/PycharmProjects/mainTest/answer_table.txt','r',encoding='utf8') as ff:
    text =ff.read()

# Regular expression to extract table name and row count
pattern = r"Is (\S+) a dictionary or reference table\? Row count = (\d+)\?"

# Find all matches in the text
matches = re.finditer(pattern, text)

# Prepare a list to store the extracted data
data = []

# Iterate through each match and append the table_name and row_count to data
for match in matches:
    table_name = match.group(1)
    row_count = int(match.group(2))
    data.append([table_name, row_count])

# Write the data to a CSV file
csv_file = 'table_row_counts.csv'
with open(csv_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    # Write the header
    writer.writerow(['table_name', 'row_count'])
    # Write the rows
    writer.writerows(data)

print(f"CSV file '{csv_file}' has been created successfully.")