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


#import requests
import httplib2
import json
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
        for batch in self.adapt_records(records):
            #requests.post(self.address + 'put', json=batch)
            self.client.request(self.address + 'put', 'POST', body=json.dumps(batch), headers=self.headers)
        return

    def query_records(self, data):
        #r = requests.post(self.address + 'query', json=data)
        #return r.text
        return data

    def adapt_records(self, records):
        list_of_data = []
        i = 1
        batch_list = []
        for record in records:

            data_point = {'metric': record['meter'], 'timestamp': record['time'],
                          'value': record['usage'], 'tags': {'host': record['node_id'],
                                                             'device_id': record['device_id']
                                                             }
                          }
            batch_list.append(data_point)
            i += 1
            if i > self.batch_size:
                list_of_data.append(batch_list)
                batch_list = []
        list_of_data.append(batch_list)
        return list_of_data
