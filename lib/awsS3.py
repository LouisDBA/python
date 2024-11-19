#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import boto3, botocore, inspect
from time import sleep

class s3Class():
    def __init__(self, profile='default', retry=3, delay=2) :
        # retry : Number of times to retry when session connection fails
        # delay : Wait time before retrying (sec)
        self.profile = profile
        self.retry = retry
        self.delay = delay
        self.s3_client = None
        
        cnt = 0
        while cnt < retry :
            try :
                self.s3_client = boto3.Session(profile_name=profile).client('s3')
                result_log = f'{profile} Get session complete.'
                print(f'{result_log}')
                break
            except botocore.exception.NoCredentialsError as e :
                cnt += 1
                exception_log = f'({cnt}/{retry}) rdsClass credential Exception log : {inspect.currentframe().f_code.co_name}, {e}'
                print(f'{exception_log}')
                sleep(delay)

    def copy_to_s3(self, local_file_path, bucket_name, s3_key=None):
        """
        Copy a local file to an S3 bucket.
        
        :param local_file_path: Path to the local file to upload
        :param bucket_name: Name of the S3 bucket
        :param s3_key: Optional custom S3 key (path). If not provided, uses the filename.
        """
        # Create S3 client
        #s3_client = boto3.client('s3')
        
        # Use filename as S3 key if not specified
        if s3_key is None:
            s3_key = os.path.basename(local_file_path)
        
        try:
            # Upload the file
            self.s3_client.upload_file(local_file_path, bucket_name, s3_key)
            print(f"Successfully uploaded {local_file_path} to s3://{bucket_name}/{s3_key}")
        except Exception as e:
            print(f"Error uploading file: {e}")

    def download_from_s3(self, bucket_name, s3_key, local_file_path=None):
        """
        Download a file from S3 to local.
        
        :param bucket_name: Name of the S3 bucket
        :param s3_key: Path/key of the file in the S3 bucket
        :param local_file_path: Optional local file path. If not provided, uses the S3 key filename.
        """
        # Create S3 client
        #s3_client = boto3.client('s3')
        
        # Use S3 key filename if local path not specified
        if local_file_path is None:
            local_file_path = os.path.basename(s3_key)
        
        try:
            # Ensure local directory exists
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            
            # Download the file
            self.s3_client.download_file(bucket_name, s3_key, local_file_path)
            print(f"Successfully downloaded s3://{bucket_name}/{s3_key} to {local_file_path}")
        except Exception as e:
            print(f"Error downloading file: {e}")

# Example usage
if __name__ == "__main__":
    # Replace with your actual local file path and bucket name
    sample = "s3://s3-database/server-data-backup/20241011/command1/aws_config"
    dml_path="s3://s3-database/work/dml"
    ddl_path="s3://s3-database/work/ddl"
    log_path="s3://s3-database/work/log"
    # 보통 Key 의 경우 s3://s3-database-slowquery 빼고 시작인데..우리는 key로 진행 안함
    # bucket : bucket name 의 경우 s3-database-slowquery
    # bucket name 의 경우 언더바, 대문자, 하이픈으로 끝나는 것을 허용하지 않음
    local_file = "/path/to/your/local/file.txt"
    bucket = "your-bucket-name"
    
    # Optional: Specify a custom S3 key/path
    # s3_key = "custom/path/in/bucket/filename.txt"
    
    copy_to_s3(local_file, bucket)

    bucket = "your-bucket-name"
    s3_key = "path/to/file/in/bucket.txt"
    
    # Optional: Specify a custom local file path
    # local_path = "/path/to/save/downloaded/file.txt"
    
    download_from_s3(bucket, s3_key)
