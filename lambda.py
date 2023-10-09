import json
import boto3

def lambda_handler(event, context):

    job_name = 'NOME_JOB'

    print(event)
    bucket = json.loads(event['Records'][0]['body'])['Records'][0]['s3']['bucket']['name']
    
    client = boto3.client('glue')
    
    response = client.start_job_run(
        JobName=job_name,
        Arguments={
            '--s3': bucket,
            '--job-bookmark-option': 'job-bookmark-enable'
        }
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('OK')
    }
