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

# Load the list of table names from an external text file
file_path = 'dblist.txt'
with open(file_path, 'r') as file:
    table_names = file.read().splitlines()  # Removes the newline character
    print(table_names)

# Display available tables
print("Available tables:")
for idx, table in enumerate(table_names):
    parts = table.split("_", 2)
    print(f"{idx + 1}. {parts[2]}")

# Prompt the user to select a table
table_index = int(input(f"Please select a table by entering the corresponding number (1-{len(table_names)}): "))
selected_table_name = table_names[table_index - 1]

# Define the query
# query = f"SELECT count(*) FROM {selected_table_name};"  # No LIMIT to fetch all rows

query = f"SELECT COUNT(DISTINCT ContactId) AS distinct_count FROM {selected_table_name};"  # No LIMIT to fetch all rows

# business_date = '2024-11-18'
# query = f"""
#     SELECT *
#     FROM {selected_table_name}
#     WHERE (CAST(create_date AS DATE) BETWEEN DATE_ADD('day', -5, DATE('{business_date}')) AND DATE('{business_date}'))
#        OR (CAST(update_date AS DATE) BETWEEN DATE_ADD('day', -5, DATE('{business_date}')) AND DATE('{business_date}'));
# """

#19
# query = f"""
#     SELECT 
#         count(DISTINCT as_customer) AS "จำนวนคนที่ Earn",
#         count(as_customer) AS "จำนวน Earn Transaction",
#         sum(as_Point) AS "จำนวน Point",
#         sum(as_Amount) AS "จำนวน Spending",
#         count(DISTINCT as_MerchantIdName) AS "จำนวนร้านค้าที่ Earn"
#     FROM {selected_table_name}
# """

# # #21
# query = f"""
#     SELECT 
#         COUNT(DISTINCT as_customer) AS "จำนวนคนที่ Burn",
#         SUM(as_point) AS "จำนวน Burn Point",
#         COUNT(DISTINCT as_PointDescription) AS "จำนวน Campaign",
#         COUNT(as_customer) AS "จำนวน Burn Transaction"
#     FROM {selected_table_name}
# """

# #22
# query = f"""
#     select count( Distinct as_UserID)as  "จำนวน User"
#     FROM {selected_table_name}
# """

# # 23
# query = f"""
#     SELECT Count(Distinct car_lno) as "จำนวนรถ"
#         ,sum(parking_amt) as "จำนวนเงินค่าจอด"
#         ,sum(discount_amt) as "จำนวนส่วนลด"
#         ,sum(exit_cash_amt) as "จำนวนเงินค่าจอดจริงหลังหักส่วนลด"
#     FROM {selected_table_name}
# """

# # 39
# query = f"""    
#     SELECT count(distinct entry_license)as "จำนวนรถ" 
#         ,sum(amount) as "ค่าจอด"
#         ,sum(discount)as "ส่วนลด"
#     FROM {selected_table_name}
# """

# 33
# query = f"""    
#     SELECT sum(total_amount) as "ค่าตอบแทนไกด์สุทธิ"
#         ,sum(person_total) as "จำนวนนักท่องเที่ยว"
#     FROM {selected_table_name}
# """

# Execute the query and fetch results
print(f"Executing query for table: {selected_table_name}")
df = handler.execute_query("scbprd_pst_catdb", query)

# Show and save the data
handler.show_for_power_bi(df)
output_filename = f"{selected_table_name}_{timestamp}.csv"
handler.save_to_csv(df, output_filename)

print(f"Data saved to {output_filename}")
