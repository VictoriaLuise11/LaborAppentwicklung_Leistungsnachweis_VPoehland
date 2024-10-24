import os
import requests
import json
from collections import defaultdict

API_URL = os.getenv('API_URL', 'http://assessment:8080')  # Nutzt eine Umgebungsvariable, standardmäßig auf 'http://assessment:8080'

def get_data():
    """
    Holt die Rohdaten von der API.
    """
    try:
        response = requests.get(f'{API_URL}/v1/dataset')
        response.raise_for_status()  # Überprüft, ob der Request erfolgreich war
        data = response.json()
        return data.get('events', [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return []

def process_data(events):
    """
    Aggregiert die Laufzeiten der Workloads pro Kunde.
    """
    workloads = {}
    customer_consumption = defaultdict(int)

    for event in events:
        workload_id = event.get('workloadId')
        customer_id = event.get('customerId')
        timestamp = event.get('timestamp')
        event_type = event.get('eventType')

        if workload_id not in workloads:
            workloads[workload_id] = {'start': None, 'stop': None, 'customerId': customer_id}

        workloads[workload_id][event_type] = timestamp

    for workload in workloads.values():
        start = workload['start']
        stop = workload['stop']
        customer_id = workload['customerId']

        if start is not None and stop is not None:
            runtime = stop - start
            customer_consumption[customer_id] += runtime

    return customer_consumption

def format_result(customer_consumption):
    """
    Formatiert die aggregierten Daten in das gewünschte Ergebnisformat.
    """
    result = {'result': []}
    for customer_id, consumption in customer_consumption.items():
        result['result'].append({
            'customerId': customer_id,
            'consumption': consumption
        })
    return result

def send_result(result):
    """
    Sendet das aggregierte Ergebnis an die API.
    """
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(f'{API_URL}/v1/result', headers=headers, data=json.dumps(result))
        response.raise_for_status()  # Überprüft, ob der Request erfolgreich war
        return response.status_code, response.text
    except requests.exceptions.RequestException as e:
        print(f"Error sending data: {e}")
        return None, str(e)

def main():
    events = get_data()
    if not events:
        print("No events found or failed to fetch data.")
        return

    customer_consumption = process_data(events)
    result = format_result(customer_consumption)
    status_code, response_text = send_result(result)

    if status_code:
        print(f'Status Code: {status_code}')
        print(f'Response: {response_text}')
    else:
        print("Failed to send results.")

if __name__ == '__main__':
    main()

