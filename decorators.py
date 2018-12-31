import time

from influxdb import InfluxDBClient

DATABASE = ""


class Client:
    def __init__(self, host, username, password, database=None, port=8086):
        self.client = InfluxDBClient(host=host, port=port, username=username, password=password, database=database)

    def create_database_if_not_exist(self):
        if DATABASE not in self.client.get_list_database():
            self.client.create_database(DATABASE)
            self.client.switch_database(DATABASE)

    def write_records(self, method, driver, value, metadata):
        self.create_database_if_not_exist()
        self.client.write_points([{'tags': {'driver': driver}, 'fields': {'result': value, 'metadata': metadata},
                                   'measurement': method}])


client = Client('localhost', 'apalia', 'apalia')


#client.create_database_if_not_exist()

#client = Client('localhost', 'apalia', 'apalia', 'results')


def time_evaluation(func):
    def timed(*args, **kw):
        metadata = {}
        if args[1]:
            records = len(args[1])
            metadata['records'] = records
        ts = time.time()
        result = func(*args, **kw)
        te = time.time()
        client.write_records(method=func.__name__, driver=args[0].__module__.split('.')[-2],
                             value=te-ts, metadata=str(metadata))
        return result
    return timed
