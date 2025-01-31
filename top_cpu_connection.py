import boto3
import datetime
from typing import Dict, Optional, Tuple

def get_aurora_metrics(cluster_identifier: str, period_hours: int = 24) -> Dict:
    """
    Retrieve maximum CPU utilization and connection metrics for an Aurora MySQL cluster.
    
    Args:
        cluster_identifier (str): The Aurora cluster identifier
        period_hours (int): Number of hours to look back for metrics (default: 24)
        
    Returns:
        Dict containing max CPU utilization and max connections
    """
    cloudwatch = boto3.client('cloudwatch')
    rds = boto3.client('rds')
    
    # Get cluster information to determine instance class
    try:
        response = rds.describe_db_clusters(DBClusterIdentifier=cluster_identifier)
        if not response['DBClusters']:
            raise ValueError(f"No cluster found with identifier {cluster_identifier}")
            
        # Get all instance identifiers in the cluster
        instance_ids = [instance['DBInstanceIdentifier'] 
                       for instance in response['DBClusters'][0]['DBClusterMembers']]
    except Exception as e:
        raise Exception(f"Error retrieving cluster information: {str(e)}")

    end_time = datetime.datetime.utcnow()
    start_time = end_time - datetime.timedelta(hours=period_hours)
    
    metrics = {}
    
    for instance_id in instance_ids:
        try:
            # Get CPU utilization
            cpu_response = cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='CPUUtilization',
                Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,  # 5-minute periods
                Statistics=['Maximum']
            )
            
            # Get database connections
            conn_response = cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='DatabaseConnections',
                Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,  # 5-minute periods
                Statistics=['Maximum']
            )
            
            # Extract maximum values
            max_cpu = max([datapoint['Maximum'] for datapoint in cpu_response['Datapoints']], default=0)
            max_connections = max([datapoint['Maximum'] for datapoint in conn_response['Datapoints']], default=0)
            
            metrics[instance_id] = {
                'max_cpu_utilization': max_cpu,
                'max_connections': max_connections
            }
            
        except Exception as e:
            print(f"Error retrieving metrics for instance {instance_id}: {str(e)}")
            continue
    
    return metrics

def main():
    # Example usage
    CLUSTER_IDENTIFIER = 'your-aurora-cluster-identifier'
    
    try:
        metrics = get_aurora_metrics(CLUSTER_IDENTIFIER)
        
        print(f"\nMetrics for Aurora Cluster: {CLUSTER_IDENTIFIER}")
        print("-" * 50)
        
        for instance_id, instance_metrics in metrics.items():
            print(f"\nInstance: {instance_id}")
            print(f"Max CPU Utilization: {instance_metrics['max_cpu_utilization']:.2f}%")
            print(f"Max Connections: {int(instance_metrics['max_connections'])}")
            
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == '__main__':
    main()
