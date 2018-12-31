from generate_records import generate_records
import decorators
from driver.influxdb.client import Client as InfluxClient
from driver.opentsdb.client import Client as OpentsDB
from driver.postgresdb.client import Client as Postgress
import time


def current_mills():
    return int(time.time()*1000.0)


results_client = decorators.Client(host='localhost', username='apalia', password='apalia')

clients = {'opentsdb': OpentsDB(host='127.0.0.1'),
           'influxdb': InfluxClient(host='localhost', database='evaluation', username='apalia', password='apalia'),
           'postgresql': Postgress(host='localhost', database='evaluation', username='apalia', password='apalia')
           }

create_experiment = {"writing_multiple_databases": {"write_records": {"records": {"nodes": 10,
                                                                                  "devices": 10,
                                                                                  "meters": 10,
                                                                                  "usage_range": 100},
                                                                      "drivers": ["opentsdb",
                                                                                  "influxdb",
                                                                                  "postgresql"],
                                                                      "number_of_experiments": 10
                                                                      }
                                                    }
                     }


def evaluate(data):
    for experiment in data:
        database = str(current_mills()) + experiment
        decorators.DATABASE = database
        for method in data[experiment]:
            if 'write' in method:
                if 'records' not in data[experiment][method]:
                    raise Exception
                records_data = data[experiment][method]['records']
                for i in range(data[experiment][method]["number_of_experiments"]):
                    records = generate_records(nodes=records_data['nodes'], devices=records_data['devices'],
                                               meters=records_data['meters'], usage_range=records_data['usage_range'])
                    for key in data[experiment][method]['drivers']:
                        getattr(clients[key], method)(records)


evaluate(data=create_experiment)


# client = OpentsDB('http://127.0.0.1', '4242')
#
# client.write_records(generate_records(10, 10, 10, 100))
#
# print('here')

# data_points = generate_records(10, 10, 10, 100)
# influxClient = InfluxClient()
# influxClient.write_records(data_points)
# print (influxClient.get_records())
#
#
# OpentsClient = OpentsDB('http://160.85.31.112', '4242')
# OpentsClient.write_records(data_points)
# print("and the answer is 42")
# print (OpentsClient.query_records({
#     "start": 0,
#
#     "queries": [
#         {
#             "aggregator": "count",
#             "metric": "meter1",
#             "tags": {
#                 "host": "*"
#             }
#         }
#         ]
#
# })
#
# )

#
#
# mydata = [
#     {
#         "metric": "meterlexxo2",
#         "timestamp": 1544005020,
#         "value": 51,
#         "tags": {
#            "host": "lexx",
#            "device_id": "device1"
#         }
#     },
#     {
#         "metric": "meterlexxo2",
#         "timestamp": 1544005021,
#         "value": 50,
#         "tags": {
#            "host": "lexx",
#            "device_id": "device1"
#         }
#     }
# ]
# print (client.write_records(mydata))
#
# print(client.query_records({
#     "start": 1356998400,
#     "end": 1544005112,
#     "queries": [
#         {
#             "aggregator": "none",
#             "metric": "meterlexxo2",
#             "tags": {
#                 "host": "lexx"
#             }
#         }
#         ]
#
# })
# )