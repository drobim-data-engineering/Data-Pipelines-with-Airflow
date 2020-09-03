import boto3
import time
import configparser
from airflow import settings
from airflow.models import Connection
from airflow.utils import db
from create_resources import config_file, s3_arn_policy, get_airflow_connection_args

# Reading cfg file
config = configparser.ConfigParser()
config.read(config_file)

# Setting up Access Key and Secret Key
AWS_KEY = config.get('AWS','AWS_ACCESS_KEY')
AWS_SECRET = config.get('AWS','AWS_SECRET_ACCESS_KEY')
AWS_REGION = config.get('AWS','REGION')

# Define AWS Services
redshift_client = boto3.client('redshift', region_name=AWS_REGION, aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)
iam_client = boto3.client('iam', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)
ec2_client = boto3.client('ec2', region_name=AWS_REGION, aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)

def delete_redshift_cluster(config):
    """Deletes AWS Redshift Cluster

    Args:
        config (ConfigParser object): Configuration File to define Resource configuration

    Returns:
        dictionary: AWS Redshift Information
    """
    try:
        response = redshift_client.delete_cluster(
            ClusterIdentifier=config.get('CLUSTER', 'CLUSTERIDENTIFIER'),
            SkipFinalClusterSnapshot=True
        )
    except:
        print("Redshift Cluster '%s' does not exist!" % (config.get('CLUSTER', 'CLUSTERIDENTIFIER')))
        return None
    else:
        return response['Cluster']

def wait_for_cluster_deletion(cluster_id):
    """Verifies if AWS Redshift Cluster was deleted

    Args:
        cluster_id (dictionary): AWS Redshift Cluster Information
    """
    while True:
        try:
            redshift_client.describe_clusters(ClusterIdentifier=cluster_id)
        except:
            break
        else:
            time.sleep(60)

def delete_iam_role(config, arn_policy):
    """Deletes AWS IAM Role

    Args:
        config (ConfigParser object): Configuration File to define Resource configuration
        arn_policy (string): ARN Policy you want to detach from the IAM Role
    """
    try:
        iam_client.detach_role_policy(
            RoleName=config.get('SECURITY', 'ROLE_NAME'),
            PolicyArn=s3_arn_policy
        )
        iam_client.delete_role(RoleName=config.get('SECURITY', 'ROLE_NAME'))
        print('IAM Role deleted.')
    except:
        print("IAM Role '%s' does not exist!" % (config.get('SECURITY', 'ROLE_NAME')))

def delete_security_group(config):
    """Deletes AWS VPC Security Group

    Args:
        config (ConfigParser object): Configuration File to define Resource configuration
    """
    try:
        ec2_client.delete_security_group(GroupId=config.get('SECURITY', 'SG_ID'))
        print('Security Group deleted.')
    except:
        print("Security Group '%s' does not exist!" % (config.get('SECURITY', 'SG_ID')))

def delete_airflow_connection(connection_args):
    """Deletes AirFlow Connection

    Args:
        connection_args (dictionary): Connection Information such as name, host, user, password
    """
    # Gets the session
    session = settings.Session()

    # Inserts List Of Available Connections On List Object
    connections = str(list(session.query(Connection).all()))

    if connection_args['conn_id'] in connections:
        try:

            # Creates a connection object
            conn = (session
                    .query(Connection)
                    .filter(Connection.conn_id == connection_args['conn_id'])
                    .one())

            # Inserts the connection object programmatically.
            session.delete(conn)
            session.commit()

            print(f"Connection {connection_args['conn_id']} deleted.")
        except Exception as e:
            print(e)
    else:
        print(f"Connection {connection_args['conn_id']} doesn't exist.")

def delete_resources():
    """Initiate Resources Deletion"""

    config = configparser.ConfigParser()
    config.read(config_file)

    cluster_info = delete_redshift_cluster(config)

    if cluster_info is not None:
        print(f'Deleting Redshift cluster: {cluster_info["ClusterIdentifier"]}')
        print(f'Redshift Cluster status: {cluster_info["ClusterStatus"]}')

        print('Waiting for Redshift cluster to be deleted...')
        wait_for_cluster_deletion(cluster_info['ClusterIdentifier'])
        print('Redshift Cluster deleted.')

    delete_iam_role(config,s3_arn_policy)

    delete_security_group(config)

    # Setting Up Airflow Connections
    print('Setting Up Airflow Connection Args...')
    connection_args = get_airflow_connection_args()

    # Creating Airflow Connections
    print(f"Deleting {connection_args['aws']['conn_id']} Connection.")
    delete_airflow_connection(connection_args['aws'])

    print(f"Deleting {connection_args['redshift']['conn_id']} Connection.")
    delete_airflow_connection(connection_args['redshift'])

if __name__ == "__main__":
    delete_resources()