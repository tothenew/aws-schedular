import argparse
import os
import yaml
import requests
from asg_helper import AsgModule
from rds_helper import RdsModule
import boto3

# Below access and secret keys can be used or removed if using with AWS_EC2 role.
access_key = ""
secret_key = ""
slack_url = ""

# rds_client = boto3.client('rds')
# asg_client = boto3.client('autoscaling')

# For creating AWS service clients using boto3 to use later.
asg_client = boto3.client('autoscaling', region_name="us-east-1", aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key, )
rds_client = boto3.client('rds', region_name="us-east-1", aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key, )
cw_client = boto3.client('cloudwatch', region_name="us-east-1", aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key,)

# Defining variables
asg_previous_data_storage_filename = "asg_data.pkl"

# To load the data from yaml file
def get_config(file):
    with open(file) as data:
        config = yaml.safe_load(data)
    return config

# To check if config file is present or not.
def check_file_availability(file_name):
    if os.path.exists(file_name):
        print("File is present in current directory")
        return True
    else:
        print("File is not present in current directory")
        return False

def send_message(text):
    payload = {
        "text": text
    }
    url = slack_url
    r = requests.post(url, json=payload)
    print(r.text)

# Main funtion
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--filename", dest="file", help="name of config file")
    parser.add_argument("-w", "--workspace", dest="workspace", help="name of config file")
    parser.add_argument("-r", "--resource", dest="resource", help="Comma seperated string of aws resource")
    parser.add_argument("-a", "--action", dest="action", choices=['start', 'stop', "uptime", "status"],
                        help="Enter action to perform")
    args = parser.parse_args()
    print("Config Filename:", args.file)
    print("Workspace:", args.workspace)
    print("Resource list:", args.resource.split(','))
    print("action:", args.action)
    workspace = args.workspace
    resources = args.resource.split(',')
    action = args.action
    config_yaml = args.file
    config = get_config(config_yaml)
    config_workspace = config['workspaces'][workspace]


    file_check = check_file_availability(config_yaml)
    asg_module = AsgModule(client=asg_client)
    rds_module = RdsModule(client=rds_client, cw_client= cw_client)

    rds_scheduler_resp = []
    asg_scheduler_resp = []

    if file_check:
        # config_resources = read_json_file()
        for res in resources:
            if res in config_workspace:
                if res == "rds":
                    rds_scheduler_resp = rds_module.schedule_rds(config_workspace[res], action)
                if res == "asg":
                    asg_scheduler_resp = asg_module.main_schedular_asg(config_workspace[res], action)
                print("Process Started at: ", datetime.now())

        data = "--------Below are the details about RDS-------\n"
        if len(rds_scheduler_resp) > 0:
            for i in rds_scheduler_resp:
                data += i + "\n"
        data += "--------Below are the details about ASG-------\n"
        if len(asg_scheduler_resp) > 0:
            for i in asg_scheduler_resp:
                data += i + "\n"
        send_message(data)
    else:
        print("Configuration file is not present in the current directory. Please check it.")


if __name__ == '__main__':
    main()
