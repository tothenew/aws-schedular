import datetime

class RdsUptimeModule:
    def __init__(self, client):
        self.client=client

    def get_instance_uptime(self, dbname):
        # Get the current time
        now = datetime.datetime.now()
        print(now)

        # Set the start time to 24 hours ago
        start_time = now - datetime.timedelta(hours=24)
        print(start_time)

        # Get the number of sample counts for CPU utilization for the past 24 hours
        response = self.client.get_metric_data(
            MetricDataQueries=[
                {
                    'Id': 'm1',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/RDS',
                            'MetricName': 'CPUUtilization',
                            'Dimensions': [
                                {
                                    'Name': 'DBClusterIdentifier',
                                    'Value': dbname
                                },
                            ]
                        },
                        'Period': 60,
                        'Stat': 'SampleCount',
                    },
                    'ReturnData': True,
                },
            ],
            StartTime=start_time,
            EndTime=now,
            MaxDatapoints=4000
        )
        # print(response)
        # Get the number of sample counts
        sample_count = response['MetricDataResults'][0]['Values']

        final_count = 0
        for i in sample_count:
            final_count += i

        # Calculate the uptime in hours
        uptime = (int(final_count) * 60) / 3600

        final_uptime = format(uptime, ".2f")

        # Print the uptime
        print(f'Uptime in last 24 hrs: {final_uptime} hours')
        return final_uptime
