#!/usr/bin/python

import boto3
import os
import logging
import sys

from operator import itemgetter


rds = boto3.client('rds')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_latest_snapshot(db_source):

    logger.info(f"get latest snapshot of: {db_source}")
    response = rds.describe_db_snapshots(DBInstanceIdentifier=db_source, SnapshotType='automated')

    sorted_keys = sorted(response['DBSnapshots'], key=itemgetter('SnapshotCreateTime'), reverse=True)
    snapshot_id = sorted_keys[0]['DBSnapshotIdentifier']

    logger.info(f"returned latest snapshot: {snapshot_id}")
    return snapshot_id

def snapshot_exist(snapshot):
    try:
        snapshot_response = rds.describe_db_snapshots(DBSnapshotIdentifier=snapshot)
        return(snapshot_response)
    except:
        return None

def copy_snapshot(snapshot):

    rds_prefix, rds_snapshot = snapshot.split(":")
    target_snapshot = rds_prefix + "-" + rds_snapshot
    check_snapshot_exist = snapshot_exist(target_snapshot)
    if check_snapshot_exist == None:
        print("copy doesn't exist, exec copy")
        rds.copy_db_snapshot(
            SourceDBSnapshotIdentifier=snapshot,
            TargetDBSnapshotIdentifier=target_snapshot)
    else:
        sys.exit("copy exist, clean exit")

    return target_snapshot

def wait_target_snapshot(target_snapshot):
    print("waiting for snapshot to copy")
    logger.info(f"waiting for snapshot to copy")
    waiter = rds.get_waiter('db_snapshot_available')
    # max lambda timeout is 15 minutes
    waiter.wait(
        DBSnapshotIdentifier=target_snapshot,
        WaiterConfig={'Delay': 30, 'MaxAttempts': 26}
    )
    print("snapshot copy complete")
    logger.info(f"snapshot copy complete")
def share_snapshot(target_snapshot, aws_shared_account):
    logger.info(f"share snapshot to another account")
    response = rds.modify_db_snapshot_attribute(
        DBSnapshotIdentifier=target_snapshot,
        AttributeName='restore',
        ValuesToAdd=[
            aws_shared_account,
        ],
    )
    logger.info(f"share snapshot to another account complete")

def lambda_handler(event, context):

    # env variables requirement
    db_source = os.environ['DB_SOURCE']
    aws_shared_account = os.environ['AWS_SHARED_ACCOUNT']

    # get latest snapshot from db
    get_snapshot = get_latest_snapshot(db_source)

    # check if snapshot already copied before copying latest automated snapshot from db
    target_snapshot = copy_snapshot(get_snapshot)

    # wait for snapshot to be copied with retry logic
    wait_target_snapshot(target_snapshot)

    # share snapshot with another aws account
    share_snapshot(target_snapshot,aws_shared_account)

