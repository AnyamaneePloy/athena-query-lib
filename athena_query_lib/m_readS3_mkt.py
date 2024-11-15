import os
import boto3
import time
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv(dotenv_path=".env.aws")
timestamp = datetime.now().strftime("%Y%m%d_%H%M")

# Get the credentials from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region_name = os.getenv('AWS_REGION')

# Initialize boto3 session with access key and secret key from .env
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

client = session.client('athena')

# Load the list of table names from an Excel file
file_path = 'dblist.txt' 
with open(file_path, 'r') as file:
    table_names = file.read().splitlines()  # Removes the newline character
    print(table_names)


# Display the table names for the user to choose
print("Available tables:")
for idx, table in enumerate(table_names):
    parts  = table.split("_",2)
    print(f"{idx + 1}. {parts[2]}")

# Ask the user to select a table by index
table_index = int(input(f"Please select a table by entering the corresponding number (1-{len(table_names)}): "))

# Get the selected table name
selected_table_name = table_names[table_index - 1]

# Define the SQL query using the user-selected table name
query = f"""
    SELECT *
    FROM scbdev_pst_catdb.{selected_table_name}
    LIMIT 100;
"""

# Execute the query
response = client.start_query_execution(
    QueryString=query,
    QueryExecutionContext={'Database': 'scbdev_pst_catdb'},
    ResultConfiguration={'OutputLocation': 's3://ap-test-report/output/dataquery/'}
)

# Retrieve the Query Execution ID
query_execution_id = response['QueryExecutionId']

# Function to check query execution status
def check_query_status(query_execution_id):
    while True:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        
        if status == 'SUCCEEDED':
            print("Query succeeded.")
            break
        elif status == 'FAILED':
            raise Exception(f"Query failed: {response['QueryExecution']['Status']['StateChangeReason']}")
        elif status == 'CANCELLED':
            raise Exception("Query was cancelled.")
        
        print("Waiting for query to complete...")
        time.sleep(2)  # Wait for 2 seconds before checking again

# Wait for the query to complete
check_query_status(query_execution_id)

# Fetch query results
result = client.get_query_results(QueryExecutionId=query_execution_id)

# Process the results and convert to DataFrame
columns = [col['Label'] for col in result['ResultSet']['ResultSetMetadata']['ColumnInfo']]
rows = []
for row in result['ResultSet']['Rows'][1:]:  # Skip the header row
    rows.append([data.get('VarCharValue', None) for data in row['Data']])

# Create DataFrame
df = pd.DataFrame(rows, columns=columns)

# Display DataFrame
print(df)

# Save the DataFrame to a CSV file
csv_file_path = "query_results.csv"
df.to_csv(csv_file_path, index=False)

print(f"Data saved to {csv_file_path}")
