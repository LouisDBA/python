import boto3
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DocumentDBManager:
    def __init__(self, profile_name='default', region='ap-northeast-2'):
        """
        Initialize DocumentDB Manager with AWS credentials
        """
        self.profile_name = profile_name
        self.region = region
        self.client = boto3.Session(profile_name=profile_name).client('docdb', region_name=region)

    def list_document_dbs(self):
        """
        List all DocumentDB clusters
        """
        try:
            response = self.client.describe_db_clusters()
            clusters = response['DBClusters']
            logger.info("DocumentDB Clusters:")
            for cluster in clusters:
                logger.info(f"Cluster ID: {cluster['DBClusterIdentifier']}")
                logger.info(f"Status: {cluster['Status']}")
                logger.info(f"Engine Version: {cluster['EngineVersion']}")
                logger.info("-" * 50)
            return clusters
        except Exception as e:
            logger.error(f"Error listing DocumentDB clusters: {str(e)}")
            raise

    def clone_document_db(self, source_cluster_name, num_instances=2, instance_type=None):
        """
        Clone a DocumentDB cluster using point-in-time restore
        Args:
            source_cluster_name (str): Name of the source cluster to clone
            num_instances (int): Number of instances (1 primary + n-1 secondaries)
            instance_type (str): Instance type for the new cluster
        """
        try:
            # Get source cluster details
            source_cluster = self.client.describe_db_clusters(
                DBClusterIdentifier=source_cluster_name
            )['DBClusters'][0]

            # Get source instance details
            source_instance = self.client.describe_db_instances(
                Filters=[
                    {
                        'Name': 'db-cluster-id',
                        'Values': [source_cluster_name]
                    }
                ]
            )['DBInstances'][0]

            # Use source instance type if not specified
            if not instance_type:
                instance_type = source_instance['DBInstanceClass']

            # Generate new cluster name
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            new_cluster_name = f"{source_cluster_name}-clone-{timestamp}"

            # Get engine version
            engine_version = source_cluster['EngineVersion']
            logger.info(f"Source cluster engine version: {engine_version}")

            # Log oplog position for engine version 4.0 and above
            if float(engine_version.split('.')[0]) >= 4:
                logger.info(f"Oplog position for clone: {source_cluster.get('LatestRestorableTime', 'N/A')}")

            # Copy source cluster tags
            source_tags = source_cluster.get('TagList', [])
            # Add clone-specific tag
            source_tags.append({
                'Key': 'SourceCluster',
                'Value': source_cluster_name
            })

            # Create clone using point-in-time restore with latest restorable time
            response = self.client.restore_db_cluster_to_point_in_time(
                TargetDBClusterIdentifier=new_cluster_name,
                SourceDBClusterIdentifier=source_cluster_name,
                RestoreType='copy-on-write',
                UseLatestRestorableTime=True,
                DBSubnetGroupName=source_cluster['DBSubnetGroup'],
                VpcSecurityGroupIds=[source_cluster['VpcSecurityGroups'][0]['VpcSecurityGroupId']],
                DBInstanceClass=instance_type,
                NumberOfNodes=num_instances,
                KmsKeyId=source_cluster.get('KmsKeyId'),
                Tags=source_tags
            )

            logger.info(f"Successfully created clone using point-in-time restore: {new_cluster_name}")
            
            # Monitor cluster status until available
            import time
            while True:
                status_response = self.client.describe_db_clusters(
                    DBClusterIdentifier=new_cluster_name
                )
                status = status_response['DBClusters'][0]['Status']
                logger.info(f"Cluster {new_cluster_name} status: {status}")
                
                if status == 'available':
                    logger.info(f"Cluster {new_cluster_name} is now available")
                    break
                
                time.sleep(5)

            return response

        except Exception as e:
            logger.error(f"Error cloning DocumentDB cluster: {str(e)}")
            raise
