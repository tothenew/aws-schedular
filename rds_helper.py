import time
import requests


class RdsModule:
    def __init__(self, client, slack_url):
        self.client = client
        self.slack_url = slack_url

    def send_message(self, text):
        payload = {
            "text": text
        }
        url = self.slack_url
        r = requests.post(url, json=payload)
        print(r.text)

# Below function is not in use at the moment.
    def describe_rds_intance_filter(self, value):
        print(value)
        response = self.client.describe_db_instances(
            DBInstanceIdentifier='string',
            Filters=[
                {
                    'Name': "db-instance-id",
                    'Values': value
                },
            ]
        )
        return response

    def describe_rds_instance(self, tags):
        rdsInstanceList = []
        response = self.client.describe_db_instances()
        for instance in response['DBInstances']:
            tagsList = instance["TagList"]
            for tag in tagsList:
                for env in tags["Environment"]:
                    if tag['Key'] == "Environment" and tag['Value'] == env:
                        identifier = instance["DBInstanceIdentifier"]
                        rdsInstanceList.append(identifier)

        return rdsInstanceList
# Below one is not in use at the moment
    def stop_rds_instance(self, rdsInstance):
        print("Stopping, RDS", rdsInstance)
        response = self.client.stop_db_instance(
            DBInstanceIdentifier=rdsInstance,
        )

# Below one is not in use at the moment
    def start_rds_instance(self, instanceName):
        response = self.client.start_db_instance(
            DBInstanceIdentifier=instanceName,
        )

    def start_db_clusture(self, instanceName):
        response = self.client.start_db_cluster(
            DBClusterIdentifier=instanceName,
        )

    def stop_db_clusture(self, instanceName):
        response = self.client.stop_db_cluster(
            DBClusterIdentifier=instanceName,
        )

# To fetch the status of the DB instance.
    def db_clusture_status(self, db_name):
        response = self.client.describe_db_clusters(
            DBClusterIdentifier=db_name
        )
        status = response['DBClusters'][0]['Status']
        return status

    def schedule_rds(self, data, action):
        db_clusture_by_name = []
        db_clusture_by_tags = []

        if 'name' in data:
            db_clusture_by_name = data['name']
        if 'tags' in data:
            db_clusture_by_tags = self.describe_rds_instance(data['tags'])

        finalDbClusterList = list(set(db_clusture_by_name + db_clusture_by_tags))
        print("Total Number of DB clusture:", finalDbClusterList)
        status_check = True
        for rdsInstance in finalDbClusterList:
            print("RDS Clusture in aws: ", rdsInstance)
            if action == "stop":
                self.stop_db_clusture(rdsInstance)

                while status_check:
                    status = self.db_clusture_status(rdsInstance)
                    time.sleep(300)
                    if status == "stopping":
                        self.send_message(rdsInstance + " Cluster is in Starting State..")
                    if status == "stopped":
                        status_check = False
                        self.send_message(rdsInstance + " Cluster is stopped !")

            if action == "start":
                self.start_db_clusture(rdsInstance)

                while status_check:
                    status = self.db_clusture_status(rdsInstance)
                    time.sleep(300)
                    if status == "starting":
                        self.send_message(rdsInstance + " Cluster is in Starting State ...")
                    if status == "available":
                        status_check = False
                        self.send_message(rdsInstance + " Cluster is Started !")
