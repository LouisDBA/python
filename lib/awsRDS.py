#!/usr/bin/python
# -*- coding: utf-8 -*-

import boto3, botocore, inspect
from time import sleep

class rdsClass():
    def __init__(self, profile, retry=3, delay=2) :
        # retry : Number of times to retry when session connection fails
        # delay : Wait time before retrying (sec)
        self.profile = profile
        self.retry = retry
        self.delay = delay
        self.rds_client = None
        
        cnt = 0
        while cnt < retry :
            try :
                self.rds_client = boto3.Session(profile_name=profile).client('rds')
                result_log = f'{profile} Get session complete.'
                print(f'{result_log}')
                break
            except botocore.exception.NoCredentialsError as e :
                cnt += 1
                exception_log = f'({cnt}/{retry}) rdsClass credential Exception log : {inspect.currentframe().f_code.co_name}, {e}'
                print(f'{exception_log}')
                sleep(delay)

    def describe_db_cluster(self, ClusterNalme) :
        clusterInfo = self.rds_client.describe_db_clusters(DBClusterIdentifier=ClusterNalme)
        return clusterInfo

    def describe_db_subnet_groups(self, DBSubnetGroupName='') :
        if DBSubnetGroupName :
            subnetGroup = self.rds_client.describe_db_subnet_groups(DBSubnetGroupName=DBSubnetGroupName)
        else:
            subnetGroup = self.rds_client.describe_db_subnet_groups()
if __name__ == "__main__":
    profile = 'default'
    cluster_name = 'aurora-mysql-louis'
    rds_client = rdsClass(profile)
    cluster_info = rds_client.describe_db_cluster(cluster_name)
    print(f'dictionary type cluster : {cluster_info}')
