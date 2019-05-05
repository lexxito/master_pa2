import time
import random
import datetime

def generate_records(context, event):
    nodes = context["nodes"]
    devices = context["devices"]
    meters = context["meters"]
    usage_range = context["usage_range"]
    driver = context["driver"]
    current_mills = int(round(time.time() * 1000))
    batch_of_data = []
    for node in range(nodes):
        for device in range(devices):
            for meter in range(meters):
                record = {'node_id': 'node' + str(node), 'device_id': 'device' + str(device), 'time': current_mills,
                          'meter': 'meteR' + str(meter), 'usage': random.uniform(0, usage_range)}
                batch_of_data.append(record)
            current_mills += 1
    if driver == 'influxdb': 
        return influxdb(batch_of_data)
    elif driver == 'postgresql': 
        return postgressql(batch_of_data)
    elif driver == 'opentsdb': 
        return opentsdb(batch_of_data)
    else:
        raise Exception

    
def influxdb(records):
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
    return {'data': list_of_data, 'metadata': {'records': len(records)}}
    
def postgressql(records):
    fields = ['device_id', 'node_id', 'meter', 'usage', 'time']
    table = 'evaluation'
    list_of_data = []
    for record in records:
        point = ()
        for field in fields:
            if field == 'time':
                value = datetime.datetime.fromtimestamp(record[field]/1000).strftime("%Y-%m-%d %H:%M:%S.%fZ")
            else:
                value = record[field]
            point = point + (value,)
        list_of_data.append(point)
    query = "INSERT INTO %s(%s) VALUES " % (table, ",".join(fields))
    data = query + '%s', list_of_data
    return {'data': data, 'metadata': {'records': len(records)}}

def opentsdb(records, batch_size = 50):
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
        if i > batch_size:
            list_of_data.append(batch_list)
            batch_list = []
            i = 0
    list_of_data.append(batch_list)
    return  {'data': list_of_data, 'metadata': {'records': len(records)}}
