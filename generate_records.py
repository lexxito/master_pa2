import time
import random


def generate_records(nodes, devices, meters, usage_range):
    current_mills = int(round(time.time() * 1000))
    batch_of_data = []
    for node in range(nodes):
        for device in range(devices):
            for meter in range(meters):
                record = {'node_id': 'node' + str(node), 'device_id': 'device' + str(device), 'time': current_mills,
                          'meter': 'meteR' + str(meter), 'usage': random.uniform(0, usage_range)}
                batch_of_data.append(record)
            current_mills += 1
    return batch_of_data
