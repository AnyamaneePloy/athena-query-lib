import boto3

# Initialize boto3 session with the correct profile
session = boto3.Session(profile_name='scbprd-admin')
athena_client = session.client('athena')

athena_client.update_work_group(
    WorkGroup='primary',
    ConfigurationUpdates={
        'ResultConfigurationUpdates': {
            'OutputLocation': 's3://scbprd-data-bucket/dataquery/'
        }
    }
)
print("Athena workgroup updated successfully.")
