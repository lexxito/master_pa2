import configparser

from influxdb import InfluxDBClient

config = configparser.ConfigParser()
config.read('conf.ini')
influx_data = config['InfluxDB']

EVALUATION_DATABASE = ''


class Client:
    def __init__(self):
        self.client = InfluxDBClient(host=influx_data['host'],
                                     username=influx_data['username'],
                                     password=influx_data['password'],
                                     )

    def write_records(self, records):
        return "Result: {0}".format(self.client.write_points(records))

    def write_evaluation_data(self, method, driver, value, metadata):
        self.switch_database(EVALUATION_DATABASE)
        self.client.write_points([{'tags': {'driver': driver}, 'fields': {'result': value, 'metadata': metadata},
                                   'measurement': method}])

    def switch_database(self, database):
        if database not in self.client.get_list_database():
            self.client.create_database(database)
        self.client.switch_database(database)

    def list_databases(self):
        return self.client.get_list_database()

    def list_of_database(self, ):
        return self.client.get_list_database()

    def delete_database(self, db):
        return self.client.drop_database(db)

    def show_measurements(self):
        return self.client.get_list_measurements()

    def query(self, query):
        res = self.client.query(query)
        return list(res.get_points())

    def get_last_results(self, measurement):
        self.switch_database(EVALUATION_DATABASE)
        return self.query("select * from " + measurement)

