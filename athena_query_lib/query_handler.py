# athena_query_lib/query_handler.py

import boto3
import time
import pandas as pd
from dotenv import load_dotenv
import os

class AthenaQueryHandler:
    def __init__(self, aws_env_path=".env.aws", aws_access_key_id=None, aws_secret_access_key=None, aws_region=None):
        # Load .env file for custom environments
        load_dotenv(dotenv_path=aws_env_path)

        # Override environment variables if explicit credentials are provided
        aws_access_key_id = aws_access_key_id or os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = aws_secret_access_key or os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_region = aws_region or os.getenv('AWS_REGION', 'ap-southeast-1')  # Default region fallback

        if not aws_access_key_id or not aws_secret_access_key:
            raise ValueError("AWS credentials must be provided either as arguments or via environment variables.")

        # Initialize AWS session
        self.session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        self.client = self.session.client('athena')
        self.output_location = 's3://ap-test-report/output/dataquery/'

        print("AWS credentials loaded successfully!")


    def execute_query(self, database, query):
        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': self.output_location}
        )
        query_execution_id = response['QueryExecutionId']
        self._wait_for_query_completion(query_execution_id)
        result = self.client.get_query_results(QueryExecutionId=query_execution_id)
        return self._process_results(result)

    def _wait_for_query_completion(self, query_execution_id):
        while True:
            response = self.client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            if status == 'SUCCEEDED':
                break
            elif status == 'FAILED':
                raise Exception(f"Query failed: {response['QueryExecution']['Status']['StateChangeReason']}")
            elif status == 'CANCELLED':
                raise Exception("Query was cancelled.")
            time.sleep(2)

    def _process_results(self, result):
        columns = [col['Label'] for col in result['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        rows = []
        for row in result['ResultSet']['Rows'][1:]:
            rows.append([data.get('VarCharValue', None) for data in row['Data']])
        return pd.DataFrame(rows, columns=columns)

    def save_to_csv(self, df, file_path):
        df.to_csv(file_path, index=False)

    def show_for_power_bi(self, df):
        print(df)
