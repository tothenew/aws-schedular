from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

schedular_summary_message = []

class AsgModule:
    def __init__(self, client):
        self.client = client

    def create_asg_schedule_action(self,asg_name, min, max, desired):
        response = self.client.put_scheduled_update_group_action(
            AutoScalingGroupName=asg_name,
            ScheduledActionName=asg_name,
            # Time=datetime(2015, 1, 1),
            StartTime=datetime(2026, 8, 1),
            EndTime=datetime(2026, 9, 1),
            # Recurrence='once',
            MinSize=min,
            MaxSize=max,
            DesiredCapacity=desired,
            # TimeZone='string'
        )

    def get_asg_scheduled_action(self, asg_name):
        check_asg = self.get_asg_by_name(asg_name)
        if check_asg['data']['desired'] > 0 and check_asg['success'] == True:
            return {"success": False, "data": asg_name + "This ASG is already Started"}

        response = self.client.describe_scheduled_actions(
            AutoScalingGroupName=asg_name
        )
        data = {"asg_name": response['ScheduledUpdateGroupActions'][0]['AutoScalingGroupName'],
                "min_value": response['ScheduledUpdateGroupActions'][0]['MinSize'],
                "max_value": response['ScheduledUpdateGroupActions'][0]['MaxSize'],
                "desired": response['ScheduledUpdateGroupActions'][0]['DesiredCapacity']}
        print(data)
        return data

    def delete_get_scheduled_action(self, asg_name):
        self.client.delete_scheduled_action(
            AutoScalingGroupName=asg_name,
            ScheduledActionName=asg_name
        )

    def get_asg_by_name(self, asgName):
        try:
            response = self.client.describe_auto_scaling_groups(
                AutoScalingGroupNames=[asgName]
            )
            data = {"asg_name": asgName, "min_size": response['AutoScalingGroups'][0]['MinSize'],
                    "max_size": response['AutoScalingGroups'][0]['MaxSize'],
                    "desired": response['AutoScalingGroups'][0]['DesiredCapacity']}
            return {"success": True, "data": data}
        except:
            return {"success": False, "data": "Schedular is failed for:" + asgName + ".Please check it"}

    def get_data_asg_by_tag(self, tags):
        asgList = []
        response = self.client.describe_auto_scaling_groups()
        for asg in response['AutoScalingGroups']:
            tagsList = asg['Tags']
            for tag in tagsList:
                for env in tags["Environment"]:
                    if tag['Key'] == "Environment" and tag['Value'] == env:
                        asgName = asg["AutoScalingGroupName"]
                        asgList.append(asgName)

        return asgList

    def check_instance_start_status(self, asgDetail):
        asgName = asgDetail['data']['asg_name']
        desired_count = asgDetail['data']['desired']
        instance_count = 0
        while instance_count != desired_count:
            print("desired Count is:", desired_count)
            print("instance Count is:", instance_count)
            asg_instance_list = self.get_asg_start_status(asgName)
            print("Instance List in: ", asgName, ": ", asg_instance_list)
            if len(asg_instance_list) > 0:
                for instance_detail in asg_instance_list:
                    if instance_detail['health_status'] == "Healthy":
                        instance_count = instance_count + 1
        schedular_summary_message.append(str(asgName) + " Asg Schedular is started !")
        return "All Instances are in:", asgName, "is Healthy"

    def get_asg_start_status(self, asgName):
        instanceList = []
        response = self.client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[asgName]
        )
        resp = response['AutoScalingGroups'][0]['Instances']
        if len(resp) > 0:
            for instance in resp:
                instanceList.append({"instance_id": instance['InstanceId'], "health_status": instance['HealthStatus']})
            return instanceList
        else:
            return []

    def get_asg_stop_status(self, asgname):
        status = True
        while status:
            response = self.client.describe_auto_scaling_groups(
                AutoScalingGroupNames=[asgname['data']['asg_name']]
            )
            resp = response['AutoScalingGroups'][0]['Instances']
            if len(resp) == 0:
                status = False
                schedular_summary_message.append(str(asgname['data']['asg_name']) + " Asg Schedular is stopped !")
        return str(asgname['data']['asg_name']) + ": Stopped Successfully !"

    def main_schedular_asg(self, data, action):
        nameAsgList = []
        tagAsgList = []
        asgDataList = []
        if 'name' in data:
            nameAsgList = data['name']
            print("names: ", nameAsgList)
        if 'tags' in data:
            tagAsgList = self.get_data_asg_by_tag(data['tags'])
            print("tags name: ", tagAsgList)
        finalAsgList = list(set(nameAsgList + tagAsgList))
        for asg in finalAsgList:
            resp = self.get_asg_by_name(asg)
            if resp['success']:
                asgDataList.append(resp)
        self.enable_disable_asg_schedular(asgDataList, action)
        return schedular_summary_message

    def enable_disable_asg_schedular(self, AsgDetailsList, action):
        if action == "start":
            print("You choose start the Schedular..")
            for live_asg in AsgDetailsList:
                print("LiveData", live_asg)
                asg_sa = self.get_asg_scheduled_action(live_asg['data']['asg_name'])
                print(asg_sa)
                if "success" in asg_sa:
                    if asg_sa['success'] == False:
                        schedular_summary_message.append(str(live_asg['data']['asg_name'])+ " is already started")
                else:
                    try:
                        self.client.update_auto_scaling_group(
                            AutoScalingGroupName=live_asg['data']['asg_name'], MinSize=asg_sa['min_value'],
                            MaxSize=asg_sa['max_value'], DesiredCapacity=asg_sa['desired'])

                        self.delete_get_scheduled_action(live_asg['data']['asg_name'])
                    except:
                        print("Issue in:" + live_asg['data']['asg_name'] + "\n---------\n")
                        schedular_summary_message.append("Issue in:" + live_asg['data']['asg_name'] + "\n---------\n")

            if len(schedular_summary_message) == 0:

                print("Process Started at: ", datetime.now())
                with ThreadPoolExecutor(max_workers=10) as executor:
                    for result in executor.map(self.check_instance_start_status, AsgDetailsList):
                        print(result)
                print("Process ended at: ", datetime.now())

        if action == "stop":
            print("You choose stop the Schedular..")
            for asg in AsgDetailsList:
                print(asg)
                self.create_asg_schedule_action(asg['data']['asg_name'], asg['data']['min_size'],
                                                asg['data']['max_size'],
                                                asg['data']['desired'])
                try:
                    self.client.update_auto_scaling_group(
                        AutoScalingGroupName=asg['data']['asg_name'], MinSize=0, MaxSize=0, DesiredCapacity=0)
                except:
                    print("Issue in:" + asg['data']['asg_name'] + "\n---------\n")

            print("Process Started at: ", datetime.now())
            with ThreadPoolExecutor(max_workers=10) as executor:
                for result in executor.map(self.get_asg_stop_status, AsgDetailsList):
                    print(result)
            print("Process ended at: ", datetime.now())
