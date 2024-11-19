import os
from athena_query_lib.query_handler import AthenaQueryHandler
from dotenv import load_dotenv
from datetime import datetime

print("AthenaQueryHandler imported successfully!")

# Load environment variables from the .env file
load_dotenv(dotenv_path=".env.aws")
timestamp = datetime.now().strftime("%Y%m%d_%H%M")

# Explicitly pass credentials
handler = AthenaQueryHandler(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_region=os.getenv("AWS_REGION")
)

# Load the list of table names from an Excel file
file_path = 'dblist.txt' 
with open(file_path, 'r') as file:
    table_names = file.read().splitlines()  # Removes the newline character
    # print(table_names)

print("Available tables:")
for idx, table in enumerate(table_names):
    parts  = table.split("_",2)
    print(f"{idx + 1}. {parts[2]}")

table_index = int(input(f"Please select a table by entering the corresponding number (1-{len(table_names)}): "))
selected_table_name = table_names[table_index - 1]

# Execute a query
query = f"SELECT * FROM {selected_table_name} LIMIT 100000;"
df = handler.execute_query("scbdev_pst_catdb", query)

handler.show_for_power_bi(df)
handler.save_to_csv(df, f"{selected_table_name}_{timestamp}.csv")
