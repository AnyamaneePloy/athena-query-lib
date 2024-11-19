import boto3
import time
import pandas as pd
from urllib.parse import urlparse
from dotenv import load_dotenv
import os

class AthenaQueryHandler:
    def __init__(self, aws_env_path=".env.aws", aws_access_key_id=None, aws_secret_access_key=None, aws_region=None, output_location=None):
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
        self.output_location = output_location or 's3://scbprd-data-bucket/dataquery/mkt/'  # Default output location

        print("AWS credentials loaded successfully!")
        print(f"Athena query results will be saved to: {self.output_location}")

    def execute_query(self, database, query):
        print(f"Executing query on database '{database}': {query}")
        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': self.output_location}
        )
        query_execution_id = response['QueryExecutionId']
        print(f"QueryExecutionId: {query_execution_id}")
        self._wait_for_query_completion(query_execution_id)
        return self._fetch_results_from_s3(query_execution_id)

    def _fetch_results_from_s3(self, query_execution_id):
        """
        Fetch query results from S3 when the dataset is large.
        """
        print("Fetching query results from S3...")
        s3_result_path = f"{self.output_location}{query_execution_id}.csv"
        print(f"Fetching results from: {s3_result_path}")

        # Parse S3 URL and download the file locally
        parsed_url = urlparse(s3_result_path)
        bucket_name = parsed_url.netloc
        key = parsed_url.path.lstrip('/')

        # Download the file
        s3 = self.session.client('s3')
        local_file = f"{query_execution_id}.csv"
        s3.download_file(bucket_name, key, local_file)
        print(f"Downloaded results to local file: {local_file}")

        # Load the results into a Pandas DataFrame
        df = pd.read_csv(local_file)
        print(f"Total rows fetched: {len(df)}")
        return df


    def _wait_for_query_completion(self, query_execution_id):
        print("Waiting for query to complete...")
        while True:
            response = self.client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            if status == 'SUCCEEDED':
                print("Query completed successfully!")
                break
            elif status == 'FAILED':
                raise Exception(f"Query failed: {response['QueryExecution']['Status']['StateChangeReason']}")
            elif status == 'CANCELLED':
                raise Exception("Query was cancelled.")
            time.sleep(2)


    def save_to_csv(self, df, file_path, encoding='utf-8-sig'):
        print(f"Saving results to CSV with encoding '{encoding}': {file_path}")
        try:
            df.to_csv(file_path, index=False, encoding=encoding)
            print("Results saved successfully!")
        except Exception as e:
            print(f"Failed to save CSV: {e}")


    def show_for_power_bi(self, df):
        print("Displaying data for Power BI:")
        print(df)
