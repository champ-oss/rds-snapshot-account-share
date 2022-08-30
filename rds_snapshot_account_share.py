#!/usr/bin/python

from __future__ import annotations

import logging
import os
import time
from operator import itemgetter
from typing import Any

import boto3

rds = boto3.client('rds')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_snapshot_tags(snapshot_arn: str) -> str | str | None:
    logger.info("checking tag value")
    tags = rds.list_tags_for_resource(ResourceName=snapshot_arn)['TagList']
    if 'copy-in-progress' in [tag['Value'] for tag in tags]:
        return "copy-in-progress"
    elif 'copy-complete' in [tag['Value'] for tag in tags]:
        return "copy-complete"
    else:
        return None


def update_snapshot_tags(arn: str, tag_value: str) -> None:
    logger.info("update snapshot tag value")
    rds.add_tags_to_resource(
        ResourceName=arn,
        Tags=[
            {
                'Key': 'rds-util-status',
                'Value': tag_value
            },
        ]
    )
    logger.info("update to snapshot tag value complete")


def get_latest_snapshot(db_source: str) -> tuple[Any, Any] | None:
    logger.info(f"get latest snapshot of: {db_source}")
    try:
        response = rds.describe_db_snapshots(DBInstanceIdentifier=db_source)
        sorted_keys = sorted(response['DBSnapshots'], key=itemgetter('SnapshotCreateTime'), reverse=True)
        snapshot_id = sorted_keys[0]['DBSnapshotIdentifier']
        snapshot_arn = sorted_keys[0]['DBSnapshotArn']

        logger.info(f"returned latest snapshot: {snapshot_id}")
        logger.info(f"returned latest snapshot arn: {snapshot_arn}")
        return snapshot_id, snapshot_arn
    except:
        logger.info("latest snapshot could be in progress")
        return


def snapshot_exist(snapshot: str) -> Any | None:
    try:
        logger.info("checking to see if snapshot exist or not")
        response = rds.describe_db_snapshots(DBSnapshotIdentifier=snapshot)
        logger.info(f"returned latest snapshot: {response}")
        return response
    except:
        logger.info("snapshot does not exist")
        return None


def delete_latest_snapshot(latest_snapshot: str) -> None:
    logger.info("delete latest snapshot")
    check_snapshot_exist = snapshot_exist(latest_snapshot)
    if check_snapshot_exist is None:
        logger.info("latest doesnt exist for delete")

    else:
        rds.delete_db_snapshot(
            DBSnapshotIdentifier=latest_snapshot,
        )
        time.sleep(5)
        logger.info("deleted latest snapshot complete")


def copy_snapshot(source_snapshot: str, target_snapshot: str) -> None:
    logger.info("copy snapshot")

    rds.copy_db_snapshot(
        SourceDBSnapshotIdentifier=source_snapshot,
        TargetDBSnapshotIdentifier=target_snapshot,
        CopyTags=True
    )
    logger.info("copy snapshot complete")


def share_snapshot(target_snapshot: str, aws_shared_account: str) -> None:
    logger.info("share snapshot to another account")
    rds.modify_db_snapshot_attribute(
        DBSnapshotIdentifier=target_snapshot,
        AttributeName='restore',
        ValuesToAdd=[
            aws_shared_account,
        ],
    )
    logger.info("share snapshot to another account complete")


def lambda_handler(event, context):
    # env variables requirement, used to share snapshot with another account
    aws_shared_account = os.environ['AWS_SHARED_ACCOUNT']

    # get RDS instances
    db_instance_list = list()
    response = rds.describe_db_instances()
    for i in response['DBInstances']:
        db_instance_name = i['DBInstanceIdentifier']
        db_instance_list.append(db_instance_name)
    for db_source in db_instance_list:
        # add suffix to snapshot, not conflict with final snapshot in place
        target_snapshot = db_source + "-latest"

        # get latest snapshot from db
        get_snapshot, snapshot_arn = get_latest_snapshot(target_snapshot)

        # check current tag on snapshot
        check_tag_value = get_snapshot_tags(snapshot_arn)

        # latest tag has been copied but not shared
        if check_tag_value == "copy-in-progress":
            # update tag before sharing with another account
            update_snapshot_tags(snapshot_arn, "copy-complete")
            # sharing snapshot with another account
            share_snapshot(get_snapshot, aws_shared_account)

        # latest tag is copied and shared, return and
        elif check_tag_value == "copy-complete":
            logger.info("nothing to do, latest snapshot copied")

        # this is an automated tag, not tagged and needs to be copied
        elif check_tag_value is None:
            delete_latest_snapshot(target_snapshot)
            update_snapshot_tags(snapshot_arn, "copy-in-progress")
            copy_snapshot(get_snapshot, target_snapshot)

        else:
            logger.warning("latest tag check failed")
    db_instance_list.clear()
