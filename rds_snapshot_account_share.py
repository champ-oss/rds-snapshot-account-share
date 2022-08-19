#!/usr/bin/python

import boto3
import os

rds = boto3.client('rds')


def get_latest_snapshot(db_source):
    source_snapshot = rds.describe_db_snapshots(
        DBInstanceIdentifier=db_source,
        SnapshotType='automated',
        MaxRecords=1)
    print(source_snapshot)
    return source_snapshot

def copy_snapshot(snapshot):
    rds_prefix, target_snapshot = snapshot.split(":")
    print(rds_prefix)
    rds.copy_db_snapshot(
        SourceDBSnapshotIdentifier=snapshot,
        TargetDBSnapshotIdentifier=target_snapshot)
    return target_snapshot

def wait_target_snapshot(target_snapshot):
    waiter = rds.get_waiter('db_snapshot_available')
    waiter.wait(
        DBInstanceIdentifier=db_identifier,
        DBSnapshotIdentifier=target_snapshot,
        WaiterConfig={'Delay': 15, 'MaxAttempts': 10}
    )

def share_snapshot(target_snapshot, aws_shared_account):
    rds.modify_db_snapshot_attribute(
        DBSnapshotIdentifier=target_snapshot,
        AttributeName='restore',
        ValuesToAdd=[
            aws_shared_account,
        ],
    )

def main():
    db_source = os.environ['DB_SOURCE']
    aws_shared_account = os.environ['AWS_SHARED_ACCOUNT']
    get_snapshot = get_latest_snapshot(db_source)
    target_snapshot = copy_snapshot(get_snapshot)
    wait_target_snapshot(target_snapshot)
    share_snapshot(target_snapshot,aws_shared_account)

main()
