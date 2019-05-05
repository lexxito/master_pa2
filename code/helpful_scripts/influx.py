from influxdb import InfluxDBClient
import time

client = InfluxDBClient(host='160.85.31.108', username='apalia',password='apalia', database='evaluation')

ts = time.time()
for i in range(10):
    query = 'select * from evaluation limit 1000000'
    data = list(client.query(query).get_points())[-1]

    query = "DELETE from evaluation where time <= '%s'" % data['time'].replace('T', ' ')[:-1]

    client.query(query)

te = time.time()


print(te-ts)

# 2018/12/28-07:52:26
#
# tsdb scan --delete 2018/12/28-07:52:26 sum meteR0



# DELETE FROM evaluation
# WHERE ctid IN (
#     SELECT ctid
#     FROM evaluation
#     LIMIT 10000000
# )