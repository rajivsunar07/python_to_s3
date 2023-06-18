import boto3
import logging
import uuid
from configparser import ConfigParser
import sys

logging.basicConfig(level=logging.INFO)

def delete_objs(bucket, objects):
    s3 = get_s3_client()
    response = s3.delete_objects(
        Bucket = bucket,
        Delete = {
            'Objects': [{'Key': obj} for obj in objects],
            'Quiet': True|False
        }
    )
    print('delete response', response)


def get_s3_client():
    config_file = '.aws/config.ini'
    config = ConfigParser()
    config.read(config_file)

    aws_access_key_id = config['credentials']['aws_access_key_id']
    aws_secret_access_key= config['credentials']['aws_secret_access_key']

    region_name = config['region']['region']

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id, 
        aws_secret_access_key=aws_secret_access_key,
        region_name = region_name
        )
    return s3

def create_bucket_if_not_exists(s3, bucket_name, delete_if_exists=False):
    if bucket_name in [x['Name'] for x in s3.list_buckets()['Buckets']]:
        if delete_if_exists:
            delete_objs(bucket_name, get_objects(bucket_name))
            res = s3.delete_bucket(Bucket=bucket_name)
            print(res)
            logging.info('Bucket Deleted')
            s3.create_bucket(Bucket=bucket_name)
            logging.info('Created new bucket')
    else:
        s3.create_bucket(Bucket=bucket_name)
        logging.info('New Bucket created')

def upload_file(file_location, bucket_name=None):
    s3 = get_s3_client()
    if bucket_name == None:
        bucket_name = str(uuid.uuid4())[:6]
    create_bucket_if_not_exists(s3, bucket_name, True)
    s3.upload_file(file_location, bucket_name, file_location)
    logging.info('File uploaded successfully to bucket:', bucket_name)

def get_objects(bucket):
    s3 = get_s3_client()
    objects= [x['Key'] for x in s3.list_objects_v2(Bucket=bucket)['Contents']]
    return objects

file_name = sys.argv[1]
upload_file(file_name, '86f44b23')
