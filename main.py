import requests
import json
from collections import defaultdict

def get_data():
    response = requests.get('http://localhost:8080/v1/dataset')
    data = response.json()
    return data['events']


def process_data(events):
    workloads = {}
    customer_consumption = defaultdict(int)

    for event in events:
        workload_id = event['workloadId']
        customer_id = event['customerId']
        timestamp = event['timestamp']
        event_type = event['eventType']

        if workload_id not in workloads:
            workloads[workload_id] = {'start': None, 'stop': None, 'customerId': customer_id}

        workloads[workload_id][event_type] = timestamp

    for workload in workloads.values():
        start = workload['start']
        stop = workload['stop']
        customer_id = workload['customerId']

        if start is not None and stop is not None:
            runtime = stop - start                                 # stop = start oder stop - start?
            customer_consumption[customer_id] += runtime

    return customer_consumption



def format_result(customer_consumption):
    result = {'result': []}
    for customer_id, consumption in customer_consumption.items():
        result['result'].append({
            'customerId': customer_id,
            'consumption': consumption
        })
    return result


def send_result(result):
    headers = {'Content-Type': 'application/json'}
    response = requests.post('http://localhost:8080/v1/result', headers=headers, data=json.dumps(result))
    return response.status_code, response.text


def main():
    events = get_data()
    customer_consumption = process_data(events)
    result = format_result(customer_consumption)
    status_code, response_text = send_result(result)
    print(f'Status Code: {status_code}')
    print(f'Response: {response_text}')


if __name__ == '__main__':
    main()

