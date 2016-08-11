import time
import sys
import boto3
import botocore


class Rds():
    def __init__(self):
        self.client = boto3.client('rds')

    def get_db_instance_class(self, instance_name):
        response = self.client.describe_db_instances()

        if response:
            instances = response.get('DBInstances')

            for instance in instances:
                identifier = instance.get('DBInstanceIdentifier')
                if identifier == instance_name:
                    instance_class = instance.get('DBInstanceClass')
                    status = instance.get('DBInstanceStatus')

                    if status == 'available':
                        print('Server is available and ready to be modified')
                        return instance_class
                    else:
                        print("Instance %s not found or not currently available" % instance_name)
                        break

        return None

    def modify_instance(self, db_identifier, target_instance_class):
        try:
            instance_class = self.get_db_instance_class(db_identifier)
            if instance_class == None:
                return
            elif instance_class == target_instance_class:
                print("Instance %s is already %s, no further modification needed." % (db_identifier, target_instance_class))
            else:
                self.client.modify_db_instance(ApplyImmediately=True,
                                               DBInstanceIdentifier=db_identifier,
                                               DBInstanceClass=target_instance_class)

        except botocore.exceptions.ClientError as e:
            print(sys.exc_info())
            raise

def main():
    # Below is for testing only
    db_identifier = 'sau-db-qa-1'
    db_instance_small = 'db.t2.large'
    db_instance_mid = 'db.m4.2xlarge'
    db_instance_large = 'db.m4.4xlarge'
    rds = Rds()
    #rds.modify_instance(db_identifier, db_instance_large)
    print("My rds current size: " + rds.get_db_instance_class(db_identifier))

if __name__ == '__main__':
    main()
