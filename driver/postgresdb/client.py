import datetime

import psycopg2
from psycopg2.extras import execute_values
from decorators import time_evaluation


class Client:
    def __init__(self, host, database, username, password):
        self.client = psycopg2.connect(host=host, database=database, user=username, password=password)
        self.cur = self.client.cursor()

    def get_records(self):
        return self.client.query('select count(*) from evaluation')

    @time_evaluation
    def write_records(self, records):
        query, points = self.adapt_records(records)
        execute_values(self.cur, query, points)
        self.client.commit()

    def adapt_records(self, records):
        fields = ['device_id', 'node_id', 'meter', 'usage', 'time']
        table = 'evaluation'
        list_of_data = []
        for record in records:
            point = ()
            for field in fields:
                if field == 'time':
                    #'2018-05-16 15:36:38'
                    value = datetime.datetime.fromtimestamp(record[field]/1000).strftime("%Y-%m-%d %H:%M:%S.%fZ")
                else:
                    value = record[field]
                point = point + (value,)
            list_of_data.append(point)
        query = "INSERT INTO %s(%s) VALUES " % (table, ",".join(fields))
        return query + '%s', list_of_data


# cl = Client(host='localhost', database='evaluation', username='apalia', password='apalia')
# data_points = generate_records(10, 10, 10, 100)
# cl.write_records(data_points)

# client = psycopg2.connect(host='localhost', database='evaluation', user='apalia', password='apalia')
#
# cur = client.cursor()
#
# my_list = [('device1', 'node1', 'meter1', 28), ('device2', 'node1', 'meter1', 29)]
#
# execute_values(cur, "INSERT INTO evaluation(device_id, node_id, meter, value) VALUES %s",
#                my_list)
#
#
# client.commit()
# cur.execute('select * from evaluation')
# db_version = cur.fetchone()
# print(db_version)
# cur.close()
# client.close()
