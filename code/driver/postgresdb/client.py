import psycopg2
from psycopg2.extras import execute_values
from timeout_decorator import timeout_decorator

from decorators import time_evaluation


class Client:
    def __init__(self, host, database, username, password):
        self.client = psycopg2.connect(host=host, database=database, user=username, password=password)
        self.cur = self.client.cursor()

    def get_records(self):
        return self.client.query('select count(*) from evaluation')

    @time_evaluation
    def aggregate_records(self, agr, field="usage", data=None):
        aggregator = agr
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
        self.cur.execute(query)
        return self.cur.fetchall()

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
        self.cur.execute(query)
        return self.cur.fetchall()

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
        self.cur.execute(query)
        return self.cur.fetchall()

    @time_evaluation
    def write_records(self, records):
        query, points = records
        execute_values(self.cur, query, points)
        self.client.commit()

    @timeout_decorator.timeout(5, use_signals=False)
    def ping(self):
        try:
            self.cur.execute('SELECT 1')
            return True
        except psycopg2.OperationalError:
            raise Exception
