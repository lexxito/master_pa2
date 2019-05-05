from influxdb import InfluxDBClient
from timeout_decorator import timeout_decorator

from decorators import time_evaluation


class Client:
    def __init__(self, host, username, password, database=None, port=8086):
        self.client = InfluxDBClient(host, port, username, password, database)

    def get_records(self):
        return self.client.query('select count(*) from evaluation')

    @time_evaluation
    def aggregate_records(self, agr, field='usage', data=None):
        aggregator = agr
        if agr == 'avg':
            aggregator = 'mean'
        cond = ''
        if data:
            cond = 'where '
            if data['meter']:
                cond += 'meter = ' + "'" + data['meter'] + "'"
            if data['host']:
                cond += ' and node_id = ' + "'" + data['host'] + "'"
            if data['device_id']:
                cond += ' and device_id = ' + "'" + data['device_id'] + "'"
        query = 'select %s(%s) from evaluation %s;' % (aggregator, field, cond)
        return list(self.client.query(query))[0][0]

    @time_evaluation
    def get_last_records(self, number, data=None):
        cond = ''
        if data:
            cond = 'where '
            if data['meter']:
                cond += 'meter = ' + "'" + data['meter'] + "'"
            if data['host']:
                cond += ' and node_id = ' + "'" + data['host'] + "'"
            if data['device_id']:
                cond += ' and device_id = ' + "'" + data['device_id'] + "'"
        query = 'select * from evaluation %s order by time desc limit %s;' % (cond, number)
        return self.client.query(query)

    @time_evaluation
    def get_first_records(self, number, data=None):
        cond = ''
        if data:
            cond = 'where '
            if data['meter']:
                cond += 'meter = ' + "'" + data['meter'] + "'"
            if data['host']:
                cond += ' and node_id = ' + "'" + data['host'] + "'"
            if data['device_id']:
                cond += ' and device_id = ' + "'" + data['device_id'] + "'"
        query = 'select * from evaluation %s order by time asc limit %s;' % (cond, number)
        return self.client.query(query)

    @time_evaluation
    def write_records(self, records):
        self.client.write_points(records)

    @timeout_decorator.timeout(5, use_signals=False)
    def ping(self):
        return self.client.ping()
