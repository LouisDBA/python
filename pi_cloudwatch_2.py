import boto3
import datetime
from botocore.exceptions import ClientError

def get_rds_metrics(db_instance_identifier, start_time=None, end_time=None):
    """
    Get RDS CPU and connection metrics using CloudWatch
    """
    try:
        cloudwatch = boto3.client('cloudwatch')
        
        if not end_time:
            end_time = datetime.datetime.utcnow()
        if not start_time:
            start_time = end_time - datetime.timedelta(hours=1)

        metrics = [
            'CPUUtilization',
            'DatabaseConnections'
        ]
        
        results = {}
        for metric in metrics:
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName=metric,
                Dimensions=[{'Name': 'DBInstanceIdentifier', 
                           'Value': db_instance_identifier}],
                StartTime=start_time,
                EndTime=end_time,
                Period=60,
                Statistics=['Maximum']
            )
            
            results[metric] = [{
                'Timestamp': datapoint['Timestamp'].isoformat(),
                'Value': datapoint['Maximum']
            } for datapoint in response['Datapoints']]
            
        return results
    except ClientError as e:
        print(f"Error accessing CloudWatch metrics: {e}")
        raise

def get_performance_insights(
    db_instance_identifier,
    start_time=None,
    end_time=None,
    metrics=['db.load.avg', 'db.sampledload.avg']
):
    """
    Retrieve RDS Performance Insights metrics for a specified DB instance.
    """
    try:
        pi_client = boto3.client('pi')
        
        if not end_time:
            end_time = datetime.datetime.utcnow()
        if not start_time:
            start_time = end_time - datetime.timedelta(hours=1)

        response = pi_client.get_resource_metrics(
            ServiceType='RDS',
            Identifier=db_instance_identifier,
            StartTime=start_time,
            EndTime=end_time,
            MetricQueries=[
                {
                    'Metric': metric,
                    'GroupBy': {
                        'Group': 'db',
                        'Dimensions': ['db.sql_tokenized'],
                        'Limit': 10
                    }
                } for metric in metrics
            ],
            PeriodInSeconds=60
        )
        
        # Get additional CloudWatch metrics
        cloudwatch_metrics = get_rds_metrics(
            db_instance_identifier,
            start_time,
            end_time
        )
        
        # Format all results
        formatted_results = {
            'DatabaseInfo': {
                'DBInstanceIdentifier': db_instance_identifier,
                'TimeRange': {
                    'StartTime': start_time.isoformat(),
                    'EndTime': end_time.isoformat()
                }
            },
            'MetricsData': {},
            'SystemMetrics': cloudwatch_metrics
        }
        
        for metric_result in response['MetricList']:
            metric_name = metric_result['Metric']
            formatted_results['MetricsData'][metric_name] = {
                'TimeSeries': [],
                'TopQueries': []
            }
            
            # Process time series data
            for datapoint in metric_result.get('DataPoints', []):
                formatted_results['MetricsData'][metric_name]['TimeSeries'].append({
                    'Timestamp': datapoint['Timestamp'].isoformat(),
                    'Value': datapoint['Value']
                })
            
            # Process top queries
            for group in metric_result.get('Groups', []):
                query_info = {
                    'QueryID': group['Group']['Value'],
                    'Metrics': group['Value']
                }
                formatted_results['MetricsData'][metric_name]['TopQueries'].append(query_info)
        
        return formatted_results
        
    except ClientError as e:
        print(f"Error accessing Performance Insights: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise

# Example usage
if __name__ == "__main__":
    try:
        # Replace with your DB instance identifier
        DB_INSTANCE = "your-db-instance-identifier"
        
        # Get metrics for the last hour
        end = datetime.datetime.utcnow()
        start = end - datetime.timedelta(hours=1)
        
        results = get_performance_insights(
            db_instance_identifier=DB_INSTANCE,
            start_time=start,
            end_time=end
        )
        
        # Print results
        print("Performance Insights Data:")
        print(f"Database: {results['DatabaseInfo']['DBInstanceIdentifier']}")
        print(f"Time Range: {results['DatabaseInfo']['TimeRange']['StartTime']} to "
              f"{results['DatabaseInfo']['TimeRange']['EndTime']}")
        
        # Print system metrics
        print("\nSystem Metrics:")
        for metric_name, datapoints in results['SystemMetrics'].items():
            print(f"\n{metric_name}:")
            for dp in sorted(datapoints, key=lambda x: x['Timestamp'])[:5]:  # Show first 5 points
                print(f"  {dp['Timestamp']}: {dp['Value']:.2f}")
        
        # Print Performance Insights metrics
        for metric_name, metric_data in results['MetricsData'].items():
            print(f"\nMetric: {metric_name}")
            print("Top Queries:")
            for query in metric_data['TopQueries']:
                print(f"  Query ID: {query['QueryID']}")
                print(f"  Value: {query['Metrics']}")
            
    except Exception as e:
        print(f"Error running script: {e}")
