from influxdb import InfluxDBClient
from decorators import time_evaluation


class Client:
    def __init__(self, host, username, password, database, port=8086):
        self.client = InfluxDBClient(host, port, username, password, database)

    def get_records(self):
        return self.client.query('select count(*) from evaluation')

    @time_evaluation
    def write_records(self, records):
        self.client.write_points(self.adapt_records(records))

    def adapt_records(self, records):
        tags = ['device_id', 'node_id', 'meter']
        measurement = 'evaluation'
        list_of_data = []
        for record in records:
            data_point = {'tags': {}, 'fields': {}, 'time': '', 'measurement': measurement}
            for key in record:
                if key in tags:
                    data_point['tags'][key] = record[key]
                elif key == 'time':
                    data_point['time'] = record[key]
                else:
                    data_point['fields'][key] = record[key]
            list_of_data.append(data_point)
        return list_of_data

