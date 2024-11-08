mport subprocess

# Define the model you're looking for
model_name = 'greenplum_dwh.ods.odata_1c.ods__odata_1c__positions'

# Run the DBT ls command to get the file associated with the model
result = subprocess.run(['dbt', 'ls', '--profiles-dir', './dbt', '--project-dir', './dbt/greenplum_dwh/', '--select', model_name], capture_output=True, text=True)

# Output the corresponding file name
print(result.stdout.strip())