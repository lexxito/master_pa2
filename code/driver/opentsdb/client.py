import httplib2
import json

import timeout_decorator

from decorators import time_evaluation


class Client:
    def __init__(self, host, port='4242'):
        address_template = '%s:%s/api/'
        if 'http' not in host:
            address_template = 'http://' + address_template
        self.address = address_template % (host, port)
        self.batch_size = 50
        self.headers = {'Content-Type': 'application/json'}
        self.client = httplib2.Http()

    @time_evaluation
    def write_records(self, records):
        for batch in records:
            self.client.request(self.address + 'put', 'POST', body=json.dumps(batch), headers=self.headers)
        return

    @time_evaluation
    def aggregate_records(self, agr, field = 'usage', data={'meter': 'meteR0', 'host': '*', 'device_id': '*'}):
        query = {
            "start": 0,
            "queries": [
                {
                    "aggregator": "none",
                    "metric": data["meter"],
                    "tags": {
                        "host": data['host'],
                        "device_id": data['device_id']
                    },
                    "downsample": "all-" + agr
                }
            ]
        }
        return self.client.request(self.address + 'query', 'POST', body=json.dumps(query), headers=self.headers)

    @time_evaluation
    def get_last_records(self, number=10000, data={'meter': 'meteR0', 'host': 'node0', 'device_id': 'device1'}):
        query = {
            "queries": [
                {
                    "metric": data["meter"],
                    "tags": {
                        "host": data['host'],
                        "device_id": data['device_id']
                    }
                }
            ],
            "backScan": 10000
        }
        return self.client.request(self.address + 'query/last', 'POST', body=json.dumps(query), headers=self.headers)

    @timeout_decorator.timeout(5, use_signals=False)
    def ping(self):
        return self.client.request(self.address, 'GET', headers=self.headers)



# import requests
#
# from opentsdb import TSDBClient
# import logging
#
# logging.basicConfig(level=logging.DEBUG)
#
#
# class Client:
#     def __init__(self, host):
#         self.client = TSDBClient(host)
#
#     def write_records(self, data):
#         for record in data:
#             self.client.send(record['meter'], record['usage'], timestamp=record['time'],
#                              device_id=record['device_id'], node_id=record['node_id'])
#
#         return
