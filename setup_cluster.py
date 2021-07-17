#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configparser
import json
import boto3  # AWS SDK (python): allows apps to interact with AWS services.
from botocore.exceptions import ClientError
from time import sleep
from pprint import pprint
import psycopg2
import sys


def create_client(DWH_REGION, AWS_KEY, AWS_SECRET):
    """
    creates clients for EC2, S3, IAM (Identify & Access Management) & Redshift.
    :param DWH_REGION: config parameter
    :param AWS_KEY: config parameter
    :param AWS_SECRET: config parameter
    :return: ec2, s3, iam (client for S3), redshift (client for Redshift) objects
    """

    print("\ncreating clients...")
    ec2 = boto3.resource('ec2',
                         region_name=DWH_REGION,
                         aws_access_key_id=AWS_KEY,
                         aws_secret_access_key=AWS_SECRET
                         )

    s3 = boto3.resource('s3',
                        region_name=DWH_REGION,
                        aws_access_key_id=AWS_KEY,
                        aws_secret_access_key=AWS_SECRET
                        )

    iam = boto3.client("iam",
                       region_name=DWH_REGION,
                       aws_access_key_id=AWS_KEY,
                       aws_secret_access_key=AWS_SECRET
                       )

    redshift = boto3.client("redshift",
                            region_name=DWH_REGION,
                            aws_access_key_id=AWS_KEY,
                            aws_secret_access_key=AWS_SECRET
                            )

    return ec2, s3, iam, redshift


def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    """
    creates IAM role for Redshift access to S3.
    :param iam: client object for IAM
    :param DWH_IAM_ROLE_NAME: string name for role
    :return: role_arn object (IAM_ROLE_ARN in dwh.cfg)
    """

    print("\ncreating iam role...")
    try:
        iam_role = iam.create_role(Path='/',
                               RoleName=DWH_IAM_ROLE_NAME,
                               Description="Allows Redshift to access S3 (read only)",
                               AssumeRolePolicyDocument=json.dumps(
                                   {
                                       'Statement':
                                           [
                                               {
                                                   'Action': 'sts:AssumeRole',
                                                   'Effect': 'Allow',
                                                   'Principal':
                                                       {
                                                           'Service': 'redshift.amazonaws.com'
                                                       }
                                               }
                                           ],
                                       'Version': '2012-10-17'
                                   }
                               )
                               )
    except Exception as e:
        print("\nexception creating iam_role: {}".format(e))

    # >>>>> attach policy to iam_role (S3 read only access)
    print("\nattaching policy...")
    try:
        response = iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )['ResponseMetadata']['HTTPStatusCode']

        # ref response error handling:
        # https://botocore.amazonaws.com/v1/documentation/api/latest/client_upgrades.html#error-handling
        if response != 200:
            print("\nerror requesting policy, output: {}".format(str(response)))
            sys.exit(1)

        role_arn = iam.get_role(
            RoleName=DWH_IAM_ROLE_NAME
        )['Role']['Arn']

        print("\nrole_arn: {}".format(str(role_arn)))

        return role_arn

    except Exception as e:
        print("\nexception attaching policy: {}".format(e))


def create_redshift_cluster(redshift,
                            DWH_CLUSTER_IDENTIFIER, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES,
                            DWH_DB_NAME, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT,
                            IAM_ROLE_ARN):
    """
    launchs Redshift cluster with given config parameters (cfg file).
    :param redshift: redshift client
    :param DWH_CLUSTER_IDENTIFIER: config parameter
    :param DWH_CLUSTER_TYPE: config parameter
    :param DWH_NODE_TYPE: config parameter
    :param DWH_NUM_NODES: config parameter
    :param DWH_DB_NAME: config parameter
    :param DWH_DB_USER: config parameter
    :param DWH_DB_PASSWORD: config parameter
    :param DWH_PORT: config parameter
    :param IAM_ROLE_ARN: role_arn object (from func create_iam_role)
    :return: cluster info/section
    """

    print("\ncreating cluster...")
    try:
        response = redshift.create_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,

            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            #PubliclyAccessible=True,

            DBName=DWH_DB_NAME,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            Port=int(DWH_PORT),

            IamRoles=[IAM_ROLE_ARN]
            )

    except ClientError as err:
        print("\nexception creating cluster, error: {}".format(err))
        return None

    else:
        return response['Cluster']


def get_cluster(redshift, DWH_CLUSTER_IDENTIFIER):
    """
    checks cluster status & gets info (important: HOST & ARN).
    :param redshift:
    :param DWH_CLUSTER_IDENTIFIER:
    :return: cluster_info, DWH_ENDPOINT, IAM_ROLE_ARN
    """

    while True:
        response = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)

        cluster_info = response['Clusters'][0]
        if cluster_info['ClusterStatus'] == 'available':
            print("\n{} is available.".format(DWH_CLUSTER_IDENTIFIER))
            break

        print("\nsetting up {}, wait...".format(DWH_CLUSTER_IDENTIFIER))
        sleep(60)

    try:
        DWH_ENDPOINT = cluster_info['Endpoint']['Address']
        IAM_ROLE_ARN = cluster_info['IamRoles'][0]['IamRoleArn']
        print("\nDWH_ENDPOINT: {}".format(DWH_ENDPOINT))
        print("\nDWH_ROLE_ARN: {}".format(IAM_ROLE_ARN))

        return cluster_info, DWH_ENDPOINT, IAM_ROLE_ARN

    except Exception as err:
        print("\nexception getting host & arn, error: {}".format(err))


def open_tcp_port(ec2, cluster_info, DWH_PORT):
    """
    opens incoming tcp port on EC2.
    :param ec2: aws resource
    :param cluster_info: cluster parameters
    :param DWH_PORT: EC2 port
    :return: defaultSg (EC2 default security group) (IAM_SG dwh.cfg)
    """

    print("\nopening tcp port...")
    try:
        vpc = ec2.Vpc(id=cluster_info['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print("\nSG: {}".format(str(defaultSg)))
        print("\nSG ID: {}".format(defaultSg.id))
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )

    except ClientError as err:
        if 'ALLOW" already exists' in err.response['Error']['Message']:
            print("\nsecurity group ok")

        else:
            print("\nexception configuring security group, error: {}".format(err))

    return defaultSg.id


def check_cluster_conn(DWH_ENDPOINT, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_DB_NAME):
    """
    checks if connection to redshift is valid.
    :param DWH_ENDPOINT: cluster endpoint
    :param DWH_DB_USER: user
    :param DWH_DB_PASSWORD: password
    :param DWH_PORT: port
    :param DWH_DB_NAME: name
    :return: none
    """

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        DWH_ENDPOINT, DWH_DB_NAME, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    print("\nconnection to redshift database is validated.")
    conn.close()


def setup_cluster():
    # gets parameters from config file dwh.cfg
    config = configparser.ConfigParser()
    config.read_file(open("dwh.cfg"))

    # prints dwh.cfg content (section: key, value)
    # pprint({s: dict(config.items(s)) for s in config.sections()})

    #####
    # pair of individual access keys
    AWS_KEY                = config.get("AWS", "AWS_KEY")
    AWS_SECRET             = config.get("AWS", "AWS_SECRET")

    # config dwh parameters
    DWH_CLUSTER_TYPE       = config.get("CLUSTER", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("CLUSTER", "DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("CLUSTER", "DWH_NODE_TYPE")
    DWH_REGION             = config.get("CLUSTER", "DWH_REGION")
    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER")
    DWH_IAM_ROLE_NAME      = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")
    DWH_DB_NAME            = config.get("CLUSTER", "DWH_DB_NAME")
    DWH_DB_USER            = config.get("CLUSTER", "DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("CLUSTER", "DWH_DB_PASSWORD")
    DWH_PORT               = config.get("CLUSTER", "DWH_PORT")

    # parameters updated in dwh.cfg after launch of cluster
    # DWH_HOST             =
    # IAM_ROLE_ARN         =
    # IAM_SG               =
    #####


    ec2, s3, iam, redshift = create_client(DWH_REGION, AWS_KEY, AWS_SECRET)


    role_arn = create_iam_role(iam, DWH_IAM_ROLE_NAME)


    cluster_info = create_redshift_cluster(redshift,
                            DWH_CLUSTER_IDENTIFIER, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES,
                            DWH_DB_NAME, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT,
                            role_arn)


    cluster_info, DWH_ENDPOINT, IAM_ROLE_ARN = get_cluster(redshift, DWH_CLUSTER_IDENTIFIER)


    IAM_SG = open_tcp_port(ec2, cluster_info, DWH_PORT)


    # update values in configuration file
    config.set("CLUSTER", "DWH_HOST", str(DWH_ENDPOINT))
    config.set("IAM_ROLE", "IAM_ROLE_ARN", str(IAM_ROLE_ARN))
    config.set("IAM_ROLE", "IAM_SG", str(IAM_SG))

    with open("dwh.cfg", 'w') as configfile:
        config.write(configfile)
    print("\nvalues of DWH_HOST, IAM_ROLE_ARN & IAM_SG updated in configuration file.")


    check_cluster_conn(DWH_ENDPOINT, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_DB_NAME)


if __name__ == '__main__':
    setup_cluster()
