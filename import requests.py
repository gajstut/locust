import requests
import json
import time
import random
import csv
import os
from datetime import datetime, timedelta
from locust import HttpUser, task, between
import urllib3

# Druid connection details
user = "admin"
password = "=="

# Output CSV file with a specific directory
output_dir = '/home/ec2-user'
os.makedirs(output_dir, exist_ok=True)

current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M')
filename = f"druid_query_results_{current_datetime}.csv"
output_file = os.path.join(output_dir, filename)

# Initialize CSV file with headers
with open(output_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Measure', 'Result', 'Response Time (ms)', 'Time_frames', 'Param_groups', 'SQL'])

# Function to generate SQL queries for different measures, time frames, and ParamId_Group values
def generate_query(time_frame, measure, param_group):
    now = datetime.now()
    end_date = datetime(2024, 7, 7)
    if time_frame == '1_hour':
        start_date = end_date - timedelta(hours=1)
    elif time_frame == '1_day':
        start_date = end_date - timedelta(days=1)
    elif time_frame == '1_week':
        start_date = end_date - timedelta(weeks=1)
    elif time_frame == '1_month':
        start_date = end_date - timedelta(days=30)

    if measure == 'MIN':
        measure_query = 'MIN("ParamValue")'
    elif measure == 'MAX':
        measure_query = 'MAX("ParamValue")'
    elif measure == 'SUM':
        measure_query = 'SUM("ParamValue")'
    elif measure == 'AVG':
        measure_query = 'AVG("ParamValue")'

    query = f"""
    SELECT
        ({measure_query} FILTER (WHERE "ParamId_Group" ='{param_group}')) AS "{param_group}_{measure}"
    FROM "satelite3"  
    WHERE (TIMESTAMP '{start_date}'<=CAST("__time" AS TIMESTAMP) AND CAST("__time" AS TIMESTAMP)<TIMESTAMP '{end_date}')
    """
    return query.strip().replace("\n", " ").replace("  ", " ")

# Function to execute the query and measure response time
def execute_query(client, query):
    url = "/druid/v2/sql"
    headers = {'Content-Type': 'application/json'}
    payload = {'query': query}
    
    start_time = time.time()
    response = client.post(url, auth=(user, password), verify=False, json=payload, headers=headers)
    end_time = time.time()
    
    response_time = (end_time - start_time) * 1000  # Convert to milliseconds
    
    if response.status_code == 200:
        return response.json(), query, response_time
    else:
        print(f"Failed to execute query: {response.status_code}")
        print(response.text)
        return None, query, response_time

class DruidUser(HttpUser):
    wait_time = between(1, 3)
    host = "https://imply-8e2-elbexter-18ndd9gbaq7de-115440412.us-east-1.elb.amazonaws.com:9088"

    # Disable SSL warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    @task
    def query_druid(self):
        time_frames = ['1_hour', '1_day', '1_week', '1_month']
        measures = ['MIN', 'MAX', 'SUM', 'AVG']
        param_groups = ['A', 'B', 'C', 'D', 'E']
        
        for time_frame in time_frames:
            for measure in measures:
                for param_group in param_groups:
                    query = generate_query(time_frame, measure, param_group)
                    result, sql_query, response_time = execute_query(self.client, query)
                    if result:
                        for key, value in result[0].items():
                            # Append results to CSV file
                            with open(output_file, mode='a', newline='') as file:
                                writer = csv.writer(file)
                                writer.writerow([f"{param_group}_{measure}", value, int(response_time), time_frame, param_group, sql_query])
                        print(f"SQL: {sql_query}")
                        for key, value in result[0].items():
                            print(f"{key}: {value}")
                        print(f"Response Time: {response_time:.2f} ms")
                    time.sleep(random.uniform(1, 3))  # Introduce delay between queries
