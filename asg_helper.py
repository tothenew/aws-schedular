import os.path
import pickle
import requests

# from aws_task.Helpers.slack_notify import SlackNotify

class AsgModule:
    def __init__(self, client, asg_previous_data_filename,slack_url):
        self.client = client
        self.asg_previous_data_filename = asg_previous_data_filename
        self.slack_url = slack_url

    def send_message(self, text):
        payload = {
            "text": text
        }
        url = self.slack_url
        r = requests.post(url, json=payload)
        print(r.text)

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

#Below comparision function is not in use at the moment.
    def compare_asg_list(self, old_list, new_list):
        unique_asg_data_list = []
        new_asg_data = new_list
        previous_data = old_list
        if len(previous_data) == 0:
            return new_list
        for old_asg in previous_data:
            for new_asg in new_asg_data:
                if old_asg['data']['asg_name'] == new_asg['data']['asg_name']:
                    pre_data_index = previous_data.index(old_asg)
                    old_asg['data']['min_size'] = new_asg['data']['min_size']
                    old_asg['data']['max_size'] = new_asg['data']['max_size']
                    old_asg['data']['desired'] = new_asg['data']['desired']
                    previous_data[pre_data_index] = old_asg
                else:
                    unique_asg_data_list.append(new_asg)
        new_data = previous_data + unique_asg_data_list
        return new_data
#

    def write_previous_data_to_file(self, asg_detail_list):

        print("write new data", asg_detail_list)
        #previous_data = self.read_pkl_file()
        #new_asg_data_list = self.compare_asg_list(previous_data, asg_detail_list)
        with open(self.asg_previous_data_filename, 'wb') as file:
            # A new file will be created
            print("Writing previous ASG data.....")
            pickle.dump(asg_detail_list, file)
            print("Writing Completed !")
        return True

    def flush_file(self):
        print("Flushing pickle file ....")
        with open(self.asg_previous_data_filename, 'wb') as file:
            pickle.dump([], file)
            print("Flushing Completed !")

    def read_pkl_file(self):
        if os.path.exists(self.asg_previous_data_filename):
            with open(self.asg_previous_data_filename, 'rb') as file:
                data = pickle.load(file)
            return data
        else:
            with open(self.asg_previous_data_filename, 'wb') as file:
                # A new file will be created
                pickle.dump([], file)
            return "New Empty file is created because no pickle files exists."

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

    def enable_disable_asg_schedular(self, AsgDetailsList, action):
        if action == "start":
            print("You choose start the Schdeular..")
            resp = self.read_pkl_file()
            for file_asg in resp:
                for live_asg in AsgDetailsList:
                    print("fileData", file_asg)
                    print("LiveData", live_asg)
                    if file_asg['data']['asg_name'] == live_asg['data']['asg_name']:
                        try:
                            self.client.update_auto_scaling_group(
                                AutoScalingGroupName=file_asg['data']['asg_name'], MinSize=file_asg['data']['min_size'],
                                MaxSize=file_asg['data']['max_size'], DesiredCapacity=file_asg['data']['desired'])
                            self.send_message(str(file_asg['data']['asg_name']) + "Asg Schedular is started !")
                        except:
                            print("Issue in:" + file_asg['data']['asg_name'] + "\n---------\n")
                            return self.send_message("Issue in:" + file_asg['data']['asg_name'] + "---------\n")
            self.flush_file()
        if action == "stop":

            print("You choose stop the Schdeular..")
            self.write_previous_data_to_file(AsgDetailsList)
            for asg in AsgDetailsList:
                try:
                    self.client.update_auto_scaling_group(
                        AutoScalingGroupName=asg['data']['asg_name'], MinSize=0, MaxSize=0, DesiredCapacity=0)
                    self.send_message(str(asg['data']['asg_name']) + "Asg Schedular is stopped !")
                except:
                    print("Issue in:" + asg['data']['asg_name'] + "\n---------\n")
                    return self.send_message("Issue in:" + asg['data']['asg_name'] + "---------\n")

