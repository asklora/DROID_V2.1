import boto3,yaml,json
import time
from datetime import datetime

class Cloud:
    def __init__(self,region='ap-east-1'):
        if not region:
            raise ValueError('region cant be none/null')
        boto3.setup_default_session(region_name=region)
        self.ec2_client = boto3.client('ec2')
        self.rds_client = boto3.client('rds')
        self.cloud_formation_client = boto3.client('cloudformation')
    
    def validate_stack(self,stack_name):
        try:
            self.cloud_formation_client.describe_stacks(StackName=stack_name)
            print('stack exist')
            return True
        except Exception as e:
            print('stack doesnt exist',e)
            return False

    def create_stack(self, template=None):
        stack_name = 'cloud-formation'
        print('checking existing stack', stack_name)
        stack_exist = self.validate_stack(stack_name)
        if not stack_exist:
            print("Creating {}".format(stack_name))
            self.cloud_formation_client.create_stack(
                StackName=stack_name,
                TemplateURL='https://s3.ap-east-1.amazonaws.com/cf-templates-ot7ai6np4d71-ap-east-1/2021060Ktf-base-networking.yml'
            )
            
            while True:
                stack = self.cloud_formation_client.describe_stacks(
                            StackName=stack_name
                        )
                stack_status = stack['Stacks'][0]['StackStatus']
                if stack_status == 'CREATE_COMPLETE':
                    print(f'{datetime.now()} status : ',stack_status)
                    print('stack created')
                    break
                    
                elif stack_status == 'CREATE_FAILED':
                    raise ValueError('somethinng went wronng')
                else:
                    print(f'{datetime.now()} status : ',stack_status)
                time.sleep(10)
        else:
            stack = self.cloud_formation_client.describe_stacks(
                            StackName=stack_name
                        )
            stack_status = stack['Stacks'][0]['StackStatus']
            print('stack exist with status ',stack_status)


class DroidDb(Cloud):

    def __init__(self):
        super().__init__()

    def create_test_db(self, create_new=False):
        while True:
            snapshot_status = self.get_snapshot()
            if snapshot_status == 'available' and create_new == False:
                print(str(datetime.now()), 'snapshot available')
                break
            elif snapshot_status == 'Not Found' or create_new:
                self.take_snapshot()
                print(str(datetime.now()), 'Creating snapshot')
                create_new = False
            else:
                print(str(datetime.now())+':'+ ' ' + "Status:", snapshot_status)

            # Wait and re-request the status to check if it already available
            time.sleep(30)
        if snapshot_status == 'available':
            if self.is_testdbexist():
                self.delete_old_testdb()
                time.sleep(3)
            self.rds_client.restore_db_cluster_from_snapshot(
                        DBClusterIdentifier='droid-v2-test-cluster',
                        SnapshotIdentifier='droid-v2-snapshot',
                        Engine='aurora-postgresql')
            while True:
                db_instance =  self.check_testdb_status()
                if db_instance == 'available':
                    break
                    print('db created')
                    return 'created'
                elif db_instance == 'creating':
                    print(f'{datetime.now()} == please wait creating test db instance ...')
                time.sleep(10)
                

    def check_testdb_status(self):
        if self.is_testdbexist():
            db = self.rds_client.describe_db_clusters(
            DBClusterIdentifier='droid-v2-test-cluster',)
            return db['DBClusters'][0]['Status']
        return "Not Found"

    def is_testdbexist(self):
        try:
            db = self.rds_client.describe_db_clusters(
                    DBClusterIdentifier='droid-v2-test-cluster',
                )
            status  = db['DBClusters'][0]['Status']
            if status == 'available':
                return True 
        except Exception as e:
            print(e)
            return False
    
    def delete_old_testdb(self):
        self.rds_client.delete_db_cluster(
                    DBClusterIdentifier='droid-v2-test-cluster',
                    SkipFinalSnapshot=True,
                )
    
    def get_snapshot(self):
        try:
            snapshot = self.rds_client.describe_db_cluster_snapshots(
                        DBClusterIdentifier='droid-v2-prod-cluster',
                        DBClusterSnapshotIdentifier='droid-v2-snapshot',
                    )
            if len(snapshot['DBClusterSnapshots'])> 0:
                status = snapshot['DBClusterSnapshots'][0]['Status']
                return status
            return "Not Found"
        except Exception as e:
            print(e)
            return "Not Found"
        
    
    
    def delete_snapshot(self):
        return self.rds_client.delete_db_cluster_snapshot(
                        DBClusterSnapshotIdentifier='droid-v2-snapshot'
                    )
    
    def take_snapshot(self):
        while True:
            snapshot = self.get_snapshot()
            if snapshot == "Not Found":
                self.rds_client.create_db_cluster_snapshot(
                    DBClusterSnapshotIdentifier='droid-v2-snapshot',
                    DBClusterIdentifier='droid-v2-prod-cluster',
                )
                return True
            elif snapshot == "available":
                self.delete_snapshot()
                self.rds_client.create_db_cluster_snapshot(
                    DBClusterSnapshotIdentifier='droid-v2-snapshot',
                    DBClusterIdentifier='droid-v2-prod-cluster',
                )
                return False
            else:
                print('snapshot status: ',snapshot)
            time.sleep(10)

        

    @property
    def prod_url(self):
        """
        return 
        -> endpoint
        -> Reader endpoint
        -> port
        """
        db = self.rds_client.describe_db_clusters(
                    DBClusterIdentifier='droid-v2-prod-cluster',
                )
        return db['DBClusters'][0]['Endpoint'],db['DBClusters'][0]['ReaderEndpoint'], db['DBClusters'][0]['Port']

    
    @property
    def test_url(self):
        """
        return 
        -> endpoint
        -> Reader endpoint
        -> port
        """
        if self.is_testdbexist():
            db = self.rds_client.describe_db_clusters(
                        DBClusterIdentifier='droid-v2-test-cluster',
                    )
            return db['DBClusters'][0]['Endpoint'],db['DBClusters'][0]['ReaderEndpoint'], db['DBClusters'][0]['Port']
        # raise ValueError('error connecting database or database not found')
