import time
import requests
from concurrent.futures import ThreadPoolExecutor
import datetime


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

    def describe_rds_intance(self, tags):
        rdsInstanceList = []
        rdsInstanceTagListFromConfig = []
        for key in tags.keys():
            rdsInstanceTagListFromConfig.append(key)
        response = self.client.describe_db_clusters()
        for instance in response['DBClusters']:
            tagsList = instance["TagList"]
            for tag in tagsList:
                for env_key, env_value in tags.items():
                    if tag['Key'] == env_key:
                        for value in env_value:
                            if tag['Value'] == value:
                                identifier = instance["DBClusterIdentifier"]
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

    def start_db_cluster(self, instanceName):
        first_status_check = self.db_cluster_status(instanceName)
        if first_status_check == "available" or first_status_check == "starting":
            return str(instanceName) + " is already Starting or Started."
        self.client.start_db_cluster(
            DBClusterIdentifier=instanceName,
        )
        status_check = True
        while status_check:
            status = self.db_cluster_status(instanceName)
            time.sleep(300)
            if status == "starting":
                self.send_message(instanceName + " Cluster is in Starting ...")
            if status == "available":
                status_check = False
                self.send_message(instanceName + " Cluster is in Started !")

    def stop_db_cluster(self, instanceName):
        first_status_check = self.db_cluster_status(instanceName)
        if first_status_check == "stopped" or first_status_check == "stopping":
            return str(instanceName) + " is already in Stopping or Stopped state."

        self.client.stop_db_cluster(
            DBClusterIdentifier=instanceName,
        )
        status_check = True
        while status_check:
            status = self.db_cluster_status(instanceName)
            time.sleep(300)
            if status == "stopping":
                self.send_message(instanceName + " Cluster is in stopping state..")
            if status == "stopped":
                status_check = False
                self.send_message(instanceName + " Cluster is in stopped !")

    # To fetch the status of the DB instance.
    def db_cluster_status(self, db_name):
        response = self.client.describe_db_clusters(
            DBClusterIdentifier=db_name
        )
        status = response['DBClusters'][0]['Status']
        return status

    def schedule_rds(self, data, action):
        db_cluster_by_name = []
        db_cluster_by_tags = []

        if 'name' in data:
            db_cluster_by_name = data['name']
        if 'tags' in data:
            db_cluster_by_tags = self.describe_rds_intance(data['tags'])

        finalDbClusterList = list(set(db_cluster_by_name + db_cluster_by_tags))
        print("Total Number of DB cluster:", finalDbClusterList)

        if action == "stop":
            print("Process started at: ", datetime.datetime.now())
            with ThreadPoolExecutor(max_workers=10) as executor:
                for result in executor.map(self.stop_db_cluster, finalDbClusterList):
                    print(result)
            print("Process ended at: ", datetime.datetime.now())

        if action == "start":
            print("Process started at: ", datetime.datetime.now())
            with ThreadPoolExecutor(max_workers=10) as executor:
                for result in executor.map(self.start_db_cluster, finalDbClusterList):
                    print(result)
            print("Process ended at: ", datetime.datetime.now())

