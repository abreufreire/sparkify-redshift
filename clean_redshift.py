#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configparser
from botocore.exceptions import ClientError
from time import sleep
from setup_cluster import create_client


def delete_redshift_cluster(redshift, DWH_CLUSTER_IDENTIFIER):
    """
    deletes running redshift cluster.
    :param redshift: redshift client
    :param DWH_CLUSTER_IDENTIFIER: config parameter
    :return: none or cluster info/section
    """

    print("\ndeleting cluster {}...".format(DWH_CLUSTER_IDENTIFIER))
    try:
        response = redshift.delete_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            SkipFinalClusterSnapshot=True
        )
    except ClientError as err:
        if 'ClusterNotFound' in str(err):
            print("\nexception: cluster does not exist")

        else:
            print("\nerror deleting cluster: {}".format(err))

        return None

    else:
        return response['Cluster']


def check_cluster_delete(redshift, DWH_CLUSTER_IDENTIFIER):
    """
    checks status of redshift cluster.
    :param redshift: client
    :param DWH_CLUSTER_IDENTIFIER: parameter
    :return: none
    """

    while True:
        try:
            response = redshift.describe_clusters(
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
        except Exception as e:
            if 'ClusterNotFound' in str(e):
                print("\nstatus: cluster deleted")
                break
            else:
                print("\nexception: {}".format(e))
        else:
            print("\nprocessing deletion, wait...")
            sleep(60)


def delete_iam_role(iam, DWH_IAM_ROLE_NAME):
    """
    detaches policy & deletes IAM role.
    :param iam: client object for IAM
    :return: none
    """
    print("\ndetaching policy...")
    try:
        iam.detach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
        print("\ndeleting iam role...")
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    except ClientError as err:
        print("\nexception with policy or iam role: {}".format(err))


def delete_security_group(ec2, IAM_SG):
    """
    deletes EC2 security group.
    :param ec2: aws resource
    :param IAM_SG: security group id parameter
    :return: none
    """

    print("\ndeleting security group...")
    try:
        ec2.delete_security_group(
            GroupId=str(IAM_SG))
    except ClientError as err:
        print("\nerror deleting security group: {}".format(err))


def clean_redshift():
    # gets parameters from configuration (dictionary)
    config = configparser.ConfigParser()
    config.read_file(open("dwh.cfg"))

    #####
    # pair of individual access keys
    AWS_KEY                = config.get("AWS", "AWS_KEY")
    AWS_SECRET             = config.get("AWS", "AWS_SECRET")

    # config dwh parameters
    DWH_REGION             = config.get("CLUSTER", "DWH_REGION")
    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER")
    DWH_IAM_ROLE_NAME      = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")
    #####


    # import create_client to get redshift object from setup_cluster
    ec2, s3, iam, redshift = create_client(DWH_REGION, AWS_KEY, AWS_SECRET)

    cluster_info = delete_redshift_cluster(redshift, DWH_CLUSTER_IDENTIFIER)

    check_cluster_delete(redshift, DWH_CLUSTER_IDENTIFIER)

    delete_iam_role(iam, DWH_IAM_ROLE_NAME)


if __name__ == '__main__':
    clean_redshift()
