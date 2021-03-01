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
            stack = self.cloud_formation_client.describe_stacks(StackName=stack_name)
            print('stack exist')
            print(stack)
            return False
        except Exception as e:
            print('stack doesnt exist')
            return True

    def create_stack(self, template=None):
        stack_name = 'cloud-formation'
        print('checking existing stack', stack_name)
        is_exist = self.validate_stack(stack_name)
        if is_exist:
            print("Creating {}".format(stack_name))
            response = self.cloud_formation_client.create_stack(
                StackName=stack_name,
                TemplateURL='https://s3.ap-east-1.amazonaws.com/cf-templates-ot7ai6np4d71-ap-east-1/2021060Ktf-base-networking.yml'
            )
            print(response)


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
        # if snapshot == 'available':
        #    self.rds_client.restore_db_cluster_from_snapshot(
        #             DBClusterIdentifier='droid-v2-test-cluster',
        #             SnapshotIdentifier='droid-v2-snapshot',
        #             Engine='aurora-postgresql')

    
    def is_testdbexist(self):
        pass

    # def get_cluster_reader_url(self):
    #     pass
    
    
    # def get_cluster_writer_url(self):
    #     pass
    
    
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

        

    # @property
    # def prod_url(self):
    #     pass
    
    # @property
    # def test_url(self):
    #     pass

# stack = cloud_formation_client.describe_stacks(StackName=stack_name)
# ec2s = ec2_client.describe_instances(Filters=[{'Name': 'tag:Name','Values': ['bastion',]},])
# rds = rds_client.describe_db_instances(DBInstanceIdentifier='droid-v2-test-cluster')
# df = pd.DataFrame(columns=['InstanceId', 'InstanceType', 'PrivateIpAddress','PublicIpAddress'])
# i = 0
# for res in ec2s['Reservations']:
#     df.loc[i, 'InstanceId'] = res['Instances'][0]['InstanceId']
#     df.loc[i, 'InstanceType'] = res['Instances'][0]['InstanceType']
#     df.loc[i, 'PrivateIpAddress'] = res['Instances'][0]['PrivateIpAddress']
#     df.loc[i, 'PublicIpAddress'] = res['Instances'][0]['PublicIpAddress']
# #     i += 1
# print (rds['DBInstances'][0]['Endpoint']['Address'])
# print (rds['DBInstances'][0]['Endpoint']['Port'])
# print (rds['DBInstances'][0]['MasterUsername'])
# print(df)