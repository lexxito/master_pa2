from driver.postgresdb.client import Client
from driver.opentsdb.client import Client as opClient
from driver.influxdb.client import Client as infClient


inf = infClient(host='localhost', database='evaluation', username='apalia', password='apalia')
postg= Client(host='localhost', database='evaluation', username='apalia', password='apalia')
opents = opClient(host='127.0.0.1')


print (inf.aggregate_records('count'))
print (inf.aggregate_records('max'))
print (inf.aggregate_records('min'))
print (inf.aggregate_records('avg'))
print (inf.aggregate_records('sum'))
print (inf.get_last_records('100'))
print (inf.get_first_records('100'))


print (postg.aggregate_records('count'))
print (postg.aggregate_records('max'))
print (postg.aggregate_records('min'))
print (postg.aggregate_records('avg'))
print (postg.aggregate_records('sum'))
print (postg.get_last_records('100'))
print (postg.get_first_records('100'))


print (postg.get_last_records('10', data ={'meter': 'meteR0', 'host': 'node1', 'device_id': 'device1'}))
print (opents.get_last_records('10', data ={'meter': 'meteR0', 'host': 'node1', 'device_id': 'device1'}))
print (inf.get_last_records('10', data ={'meter': 'meteR0', 'host': 'node0', 'device_id': 'device0'}))

print (postg.get_first_records('10', data ={'meter': 'meteR0', 'host': 'node1', 'device_id': 'device1'}))
print (inf.get_first_records('10', data ={'meter': 'meteR0', 'host': 'node1', 'device_id': 'device1'}))



