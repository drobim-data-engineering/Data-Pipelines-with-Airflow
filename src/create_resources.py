import boto3
import time
import json
import configparser
from airflow import settings
from airflow.models import Connection
from airflow.utils import db
from botocore.exceptions import ClientError

# Define config_file
config_file = 'dwh.cfg'

# Reading cfg file
config = configparser.ConfigParser()
config.read(config_file)

# Setting up Access Key and Secret Key
AWS_KEY = config.get('AWS','AWS_ACCESS_KEY')
AWS_SECRET = config.get('AWS','AWS_SECRET_ACCESS_KEY')
AWS_REGION = config.get('AWS','REGION')

# Define policy to be attached to IAM role
s3_arn_policy = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'

# Define AWS Services
redshift_client = boto3.client('redshift', region_name=AWS_REGION, aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)
iam_client = boto3.client('iam', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)
ec2_client = boto3.client('ec2', region_name=AWS_REGION, aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)

def update_config_file(config_file, section, key, value):
    """Writes to an existing config file

    Args:
        config_file (ConfigParser object): Configuration File the user wants to update
        section (string): The section on the config file the user wants to write
        key (string): The key the user wants to write
        value (string): The value the user wants to write
    """
    try:
        # Reading cfg file
        config = configparser.ConfigParser()
        config.read(config_file)

        #Setting  Section, Key and Value to be write on the cfg file
        config.set(section, key, value)

        # Writting to cfg file
        with open(config_file, 'w') as f:
            config.write(f)
    except ClientError as e:
        print(f'ERROR: {e}')

def create_iam_role(config, arn_policy):
    """Creates IAM Role on AWS

    Args:
      config (ConfigParser object): Configuration File to define Resource configuration
      arn_policy (string): ARN Policy you want to attach to the IAM Role

    Returns:
      dictionary: IAM Role Information
    """
    try:
        response = iam_client.get_role(RoleName=config.get('SECURITY', 'ROLE_NAME'))
        print('IAM Role already exists: ' + response['Role']['Arn'])
        return response
    except:
        response = None

    if response is None:
        try:
            role = iam_client.create_role(
            RoleName = config.get('SECURITY', 'ROLE_NAME'),
            Description = 'Allows Redshift to call AWS services on your behalf',
            AssumeRolePolicyDocument = json.dumps({
                'Version': '2012-10-17',
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}
                    }]
                })
            )
            iam_client.attach_role_policy(
                RoleName = config.get('SECURITY', 'ROLE_NAME'),
                PolicyArn = arn_policy
            )
            print('IAM Role Created: %s.' % (config.get('SECURITY', 'ROLE_NAME')))
            return role
        except ClientError as e:
          print(e)

def create_cluster_security_group():
  """Creates VPC Security Group on AWS

  Returns:
      string: Security Group ID
  """
  try:
    response = ec2_client.describe_security_groups(Filters= [{"Name": "group-name", "Values": [config.get('SECURITY', 'SG_Name')]}])
  except ClientError as e:
     print(e)

  if len(response['SecurityGroups']) > 0:
    print('Security Group already exists: ' + response['SecurityGroups'][0]['GroupId'])
    return response['SecurityGroups'][0]['GroupId']
  else:
    response = None

  if response is None:
    vpc_id = config.get('SECURITY', 'VPC_ID')
    if vpc_id == "":
      response = ec2_client.describe_vpcs()
      vpc_id = response.get('Vpcs', [{}])[0].get('VpcId', '')

    try:
        response = ec2_client.create_security_group(GroupName=config.get('SECURITY', 'SG_Name'),Description='Redshift security group',VpcId=vpc_id)
        security_group_id = response['GroupId']
        print('Security Group Created %s in vpc %s.' % (security_group_id, vpc_id))

        ec2_client.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {'IpProtocol': 'tcp',
                 'FromPort': 80,
                 'ToPort': 80,
                 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                {'IpProtocol': 'tcp',
                 'FromPort': 5439,
                 'ToPort': 5439,
                 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}
            ])
        return security_group_id
    except ClientError as e:
        print(e)

def create_redshift_cluster(config, iam_role_arn, cluster_sg_id):
   """Creates an Amazon Redshift cluster on AWS

  Args:
      config (ConfigParser object): Configuration File to define Resource configuration
      iam_role_arn (string): AWS IAM role to attached on Cluster
      cluster_sg_id (string): AWS VPC Security Group ID

  Returns:
      dictionary: AWS Redshift Cluster Information
  """
   try:
     response = redshift_client.describe_clusters(ClusterIdentifier=config.get('CLUSTER', 'CLUSTERIDENTIFIER'))
     print('Redshift Cluster already exists: ' + response['Clusters'][0]['ClusterIdentifier'])
     return None
   except:
     response = None

   if response is None:
     try:
       response = redshift_client.create_cluster(
       ClusterIdentifier=config.get('CLUSTER', 'CLUSTERIDENTIFIER')
       ,ClusterType=config.get('CLUSTER', 'CLUSTERTYPE')
       ,NumberOfNodes=config.getint('CLUSTER', 'NUMBEROFNODES')
       ,NodeType=config.get('CLUSTER', 'NODETYPE')
       ,PubliclyAccessible=True
       ,DBName=config.get('CLUSTER', 'DB_NAME')
       ,MasterUsername=config.get('CLUSTER', 'DB_USER')
       ,MasterUserPassword=config.get('CLUSTER', 'DB_PASSWORD')
       ,Port=config.getint('CLUSTER', 'DB_PORT')
       ,IamRoles=[iam_role_arn]
       ,VpcSecurityGroupIds=[cluster_sg_id]
       )
       return response['Cluster']
     except ClientError as e:
       print(f'ERROR: {e}')
       return None

def wait_for_cluster_creation(cluster_id):
    """Verifies if AWS Redshift Cluster was created

    Args:
      cluster_id (string): AWS Redshift Cluster Name

    Returns:
      dictionary: AWS Redshift Cluster Information
    """
    while True:
        response = redshift_client.describe_clusters(ClusterIdentifier=cluster_id)
        cluster_info = response['Clusters'][0]
        if cluster_info['ClusterStatus'] == 'available':
            break
        time.sleep(60)

    return cluster_info

def create_airflow_connection(connection_args):
    """Creates Airflow Connection

    Args:
        connection_args (dictionary): Connection Information such as name, host, user, password
    """
    # Gets the session
    session = settings.Session()

    # Inserts List Of Available Connections On List Object
    connections = str(list(session.query(Connection).all()))

    if connection_args['conn_id'] not in connections:
        try:

            # Creates a connection object
            conn = Connection(
                    conn_id=connection_args['conn_id'],
                    conn_type=connection_args['conn_type'],
                    host=connection_args['host'],
                    schema=connection_args['schema'],
                    login=connection_args['login'],
                    password=connection_args['password'],
                    port=connection_args['port']
            )

            # Inserts the connection object programmatically.
            session.add(conn)
            session.commit()

            print(f"Connection {connection_args['conn_id']} has been created.")
        except Exception as e:
            print(e)
    else:
        print(f"Connection {connection_args['conn_id']} already exists.")

def get_airflow_connection_args():
    """Defines and returns Connection Information

    Returns:
        dictionary: Connection Information such as name, host, user, password
    """
    connection_args = {
    'redshift':
        {
        'conn_id': 'redshift'
            ,'conn_type': 'postgres'
            ,'host': config.get('CLUSTER', 'HOST')
            ,'schema': config.get('CLUSTER', 'DB_NAME')
            ,'login': config.get('CLUSTER', 'DB_USER')
            ,'password': config.get('CLUSTER', 'DB_PASSWORD')
            ,'port': config.getint('CLUSTER', 'DB_PORT')
        },
        'aws':
        {
            'conn_id': 'aws_credentials'
            ,'conn_type': 'aws'
            ,'host': None
            ,'schema': None
            ,'login': config.get('AWS', 'AWS_ACCESS_KEY')
            ,'password': config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
            ,'port': None
        }
    }

    return connection_args

def create_resources():
    """Initiate Resources Creation"""

    config = configparser.ConfigParser()
    config.read(config_file)

    iam_role = create_iam_role(config, s3_arn_policy)
    cluster_sg_id = create_cluster_security_group()
    cluster_info = create_redshift_cluster(config, iam_role['Role']['Arn'], cluster_sg_id)

    if cluster_info is not None:
        print(f'Creating cluster: {cluster_info["ClusterIdentifier"]}')
        print(f'Cluster status: {cluster_info["ClusterStatus"]}')
        print(f'Database name: {cluster_info["DBName"]}')

        print('Waiting for cluster to be created...')
        cluster_info = wait_for_cluster_creation(cluster_info['ClusterIdentifier'])
        print(f'Cluster created.')
        print(f"Endpoint={cluster_info['Endpoint']['Address']}")

        # Writing to .cfg file
        print('Updatting CFG file...')
        update_config_file(config_file, 'CLUSTER', 'HOST', cluster_info['Endpoint']['Address'])
        update_config_file(config_file, 'SECURITY', 'ROLE_ARN', iam_role['Role']['Arn'])
        update_config_file(config_file, 'SECURITY', 'SG_ID', cluster_sg_id)
        print('CFG file Updated.')

        # Setting Up Airflow Connections
        print('Setting Up Airflow Connections...')
        connection_args = get_airflow_connection_args()

        # Creating Airflow Connections
        print(f"Creating {connection_args['aws']['conn_id']} Connection.")
        create_airflow_connection(connection_args['aws'])

        print(f"Creating {connection_args['redshift']['conn_id']} Connection.")
        create_airflow_connection(connection_args['redshift'])

if __name__ == "__main__":
    create_resources()