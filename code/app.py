import time
import boto3
import database

from celery_client import make_celery, generate_uuid
from flask import Flask, request, json, jsonify

from driver.influxdb.client import Client as InfluxClient
from driver.opentsdb.client import Client as OpentsClient
from driver.postgresdb.client import Client as PostgresClient


app = Flask(__name__)

app.config.update(
    CELERY_BROKER_URL='amqp://apalia:apalia@localhost:5672/apalia',
    CELERY_RESULT_BACKEND='amqp://apalia:apalia@localhost:5672/apalia'
)
celery = make_celery(app)

aws_client = boto3.client('lambda')

influx_client = database.Client()

REGISTRY = {'influxdb': InfluxClient, 'opentsdb': OpentsClient, 'postgresql': PostgresClient}


def current_mills():
    return int(time.time()*1000.0)


@app.route('/methods', methods=['GET', 'POST'])
def method_registry():
    influx_client.switch_database('registry')
    if request.method == 'POST':
        data = json.loads(request.data)
        arn = data['arn']
        name_id = data['name_id']
        names = influx_client.query("SELECT name_id FROM methods")
        for item in names:
            if name_id in item['name_id']:
                return json.dumps('Method %s already exists' % name_id)
        output = influx_client.write_records([
            {'tags': {},
                'fields': {
                    'arn': arn,
                    'name_id': name_id
                },
                'measurement': 'methods'
             }
        ])
        return jsonify(output)

    if request.method == 'GET':
        output = influx_client.query("SELECT * FROM methods;")
        return jsonify(output)


@app.route('/dbs', methods=['GET', 'POST'])
def database_registry():
    influx_client.switch_database('registry')
    if request.method == 'POST':
        data = json.loads(request.data)
        driver = data['driver']
        kwargs = data['arguments']
        name_id = data['name_id']
        names = influx_client.query("SELECT name_id FROM dbs")
        for item in names:
            if name_id in item['name_id']:
                return json.dumps('Database %s already exists' % name_id)
        client = REGISTRY[driver](**kwargs)
        try:
            ping_result = getattr(client, 'ping')()
        except:
            return json.dumps("Connection to database is not established")
        if ping_result:
            output = influx_client.write_records([
                {'tags': {},
                 'fields': {
                     'driver': driver,
                     'name_id': name_id,
                     'arguments': json.dumps(kwargs)
                 },
                 'measurement': 'dbs'
                 }
            ])
            return json.dumps(output)

        return jsonify("Database with this parameters is not valid")

    if request.method == 'GET':
        output = influx_client.query("SELECT * FROM dbs;")
        return jsonify(output)


@celery.task
def run_experiment(data, task_id):
    """Background task to send an email with Flask-Mail."""
    end_result = {}
    for experiment in data:
        end_result[experiment] = {}
        database.EVALUATION_DATABASE = '%s.%s.%s' % (str(current_mills()), task_id, experiment)
        for method in data[experiment]:
            end_result[experiment][method] = {}
            kwargs = {}
            dbs = []
            number_of_experiments = 0
            functions = {}
            for key in data[experiment][method]:
                if key == 'dbs':
                    dbs = data[experiment][method][key]
                elif key == 'number_of_experiments':
                    number_of_experiments = data[experiment][method][key]
                elif 'method' in data[experiment][method][key]:
                    influx_client.switch_database('registry')
                    query = "SELECT arn FROM methods WHERE name_id = '%s';" % data[experiment][method][key]['method']
                    response = influx_client.query(query)
                    arn = ''
                    for item in response:
                        arn = item['arn']
                    functions[key] = {arn: data[experiment][method][key]['parameters']}
                else:
                    custom_kwargs = {key: data[experiment][method][key]}
                    kwargs = {**kwargs, **custom_kwargs}
            for db in dbs:
                influx_client.switch_database('registry')
                query = "SELECT * FROM dbs WHERE name_id = '%s';" % db
                response = influx_client.query(query)
                db = {}
                for item in response:
                    db = item
                db_client = REGISTRY[db['driver']](**json.loads(db['arguments']))
                for i in range(number_of_experiments):
                    arn_kwargs = {'metadata': []}
                    for fnc in functions:
                        for arn in functions[fnc]:
                            body = functions[fnc][arn]
                            body['driver'] = db['driver']
                            response = aws_client.invoke(FunctionName=arn,
                                                         InvocationType='RequestResponse',
                                                         Payload=json.dumps(body), )
                            string_response = response["Payload"].read().decode()
                            parsed_response = json.loads(string_response)
                            arn_kwargs[fnc] = parsed_response['data']
                            if 'metadata' in parsed_response:
                                arn_kwargs['metadata'].append(parsed_response['metadata'])
                    kwargs = {**kwargs, **arn_kwargs}
                    getattr(db_client, method)(**kwargs)
                end_result[experiment][method] = database.Client().get_last_results(method)
    return end_result


@app.route('/experiments', methods=['GET', 'POST'])
def schedule_experiment():
    if request.method == 'POST':
        data = json.loads(request.data)
        task_id = generate_uuid()
        run_experiment.apply_async((data, task_id), task_id=task_id)
        return jsonify("Experiment is scheduled " + task_id)
    elif request.method == 'GET':
        dbs = influx_client.list_of_database()
        final_dict = []
        for db in dbs:
            if db['name'].startswith('1'):
                db_split = db['name'].split('.')
                final_dict.append({'time': db_split[0], 'task_id': db_split[1], 'name': db_split[2]})
        return jsonify(final_dict)


@app.route('/experiment/<task_id>')
def task_status(task_id):
    task = run_experiment.AsyncResult(task_id)
    try:
        return jsonify(task.result)
    except:
        return jsonify("Error while running experiment, please check logs")


if __name__ == '__main__':
    app.run()


# clients = {'opentsdb': OpentsDB(host='127.0.0.1'),
#            'influxdb': InfluxClient(host='localhost', database='evaluation', username='apalia', password='apalia'),
#            'postgresql': Postgress(host='localhost', database='evaluation', username='apalia', password='apalia')
#            }
