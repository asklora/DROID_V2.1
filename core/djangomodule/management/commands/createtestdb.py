from django.core.management.base import BaseCommand, CommandError
import boto3,yaml,json
import pandas as pd


class Command(BaseCommand):

    def handle(self, *args, **options):
        boto3.setup_default_session(region_name='ap-east-1')
        rds_client = boto3.client('rds')
        # snapshot = rds_client.create_db_cluster_snapshot(
        #                                             DBClusterSnapshotIdentifier='droid-prod',
        #                                             DBClusterIdentifier='droid-v2-test-cluster',
        #                                         )
        replica = rds_client.restore_db_cluster_from_snapshot(
    DBClusterIdentifier='droid-test',
    SnapshotIdentifier='droid-prod',
    Engine='aurora-postgresql'
)
        print(replica)

