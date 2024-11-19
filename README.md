# Athena Query Library (`athena-query-lib`)

This library allows you to interact with AWS Athena to execute queries and manage data efficiently.

---
🔗 https://pypi.org/project/athena-query-lib/
## 📑 Prerequisites

1. **AWS Account**: Ensure you have an active AWS account.
2. **AWS Credentials**: Have your AWS Access Key ID, Secret Access Key, and Region ready.
3. **Python Environment**:
   - Python 3.9 or later installed.
   - Virtual environment setup recommended.
4. **Dependencies**:
   Install the required Python packages:
   ```bash
   pip install athena-query-lib python-dotenv pandas
   ```
5. **Required Files**:
   1. Create a .env.aws File -> to store your AWS credentials securely.
     - Place your AWS credentials in the .env.aws file in the following format:
    ```bash
    AWS_ACCESS_KEY_ID=your_access_key_id
    AWS_SECRET_ACCESS_KEY=your_secret_access_key
    AWS_REGION=your_aws_region
    ```
    2. Prepare the dblist.txt File
     - List the table names you want to query, one per line:
     ```bash
      scbprd_pst_catdb_table1
      scbprd_pst_catdb_table2
      scbprd_pst_catdb_table3
     ```
## 🚩 How to Use
Here’s a step-by-step guide to using athena-query-lib:
1. **Import Required Modules**
   ```bash
    import os
    from athena_query_lib.query_handler import AthenaQueryHandler
    from dotenv import load_dotenv
    from datetime import datetime
   ```
2. **Initialize the Handler**
   Load AWS credentials and initialize the AthenaQueryHandler:
    ```bash
    # Load environment variables
    load_dotenv(dotenv_path=".env.aws")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    # Initialize handler
    handler = AthenaQueryHandler(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_region=os.getenv("AWS_REGION")
    )
    ```
3. **Load and Display Table Names**
   Read the table names from dblist.txt and display them for user selection:
    ```bash
    file_path = 'dblist.txt'
    with open(file_path, 'r') as file:
        table_names = file.read().splitlines()
    
    print("Available tables:")
    for idx, table in enumerate(table_names):
        parts = table.split("_", 2)
        print(f"{idx + 1}. {parts[2]}")
    
    table_index = int(input(f"Please select a table by entering the corresponding number (1-{len(table_names)}): "))
    selected_table_name = table_names[table_index - 1]
    ```
4. **Execute a Query**
  Define and run a query on the selected table:
    ```bash
    # table 22: example query
    query = f"SELECT COUNT(DISTINCT ContactId) AS distinct_count FROM {selected_table_name};"
    print(f"Executing query for table: {selected_table_name}")
    df = handler.execute_query("scbprd_pst_catdb", query)
    
    ```
5. **Display and Save Results**
   Display the query results and save them to a CSV file:
    ```bash
    handler.show_for_power_bi(df)
    output_filename = f"{selected_table_name}_{timestamp}.csv"
    handler.save_to_csv(df, output_filename)
    print(f"Data saved to {output_filename}")
    ```



