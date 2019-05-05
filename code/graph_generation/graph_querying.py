import plotly.plotly as py
import numpy as np
py.sign_in('aleksey.sergien', 'WRBJNVEVssmXHxrKRuSS')
import plotly.graph_objs as go
import database


client = database.Client()

name_exp_DEFAULT = ["sum_part", "avg_part", "count_part"]
name_databases_DEFAULT = ["influxdb", "opentsdb"]
display_DEFAULT = {'type': 'line', 'error': True}
data_start_DEFAULT = 1
data_end_DEFAULT = None
names_DEFAULT = ['driver',  'meter', 'number']
title_DEFAULT = 'Records writing'
x_title_DEFAULT = 'number of records'
y_title_DEFAULT = 'speed of writing, records per second'
file_name_DEFAULT = 'multiple'


def generate(name_exp=name_exp_DEFAULT, name_databases=name_databases_DEFAULT, display=display_DEFAULT,
             data_start=data_start_DEFAULT, data_end=data_end_DEFAULT, names=names_DEFAULT, title=title_DEFAULT,
             x_title=x_title_DEFAULT, y_title=y_title_DEFAULT, file_name=file_name_DEFAULT):
    final_dict = normalize_data(name_exp, name_databases, data_start, data_end)
    plot_data = []
    if display['type'] == 'line':
        for db in final_dict:
            ch_data = final_dict[db]
            for key in ch_data:
                data = ch_data[key]
                x = []
                y = []
                error = []
                for point in data:
                    nparr = np.array(data[point])
                    x.append(float(point))
                    y.append(nparr.mean())
                    if display['error']:
                        error.append(nparr.std() - nparr.mean())
                plot_data.append(go.Scatter(
                    x=x,
                    y=y,
                    name=make_name(names=names, driver=db, meter=key),
                    error_y=dict(
                        type='data',
                        array=error,
                        visible=True
                    )))

    elif display['type'] == 'violin':
        for db in final_dict:
            for exp in final_dict[db]:
                for key in final_dict[db][exp]:
                    trace = {
                        "type": 'violin',
                        "y": final_dict[db][exp][key],
                        "box": {
                            "visible": True
                        },
                        "meanline": {
                            "visible": True
                        },
                        "name": key + exp + db
                    }
                    plot_data.append(trace)

    layout = go.Layout(
        title=title,
        xaxis=dict(
            title=x_title,
            titlefont=dict(
                family='Courier New, monospace',
                size=18,
                color='#7f7f7f'
            )
        ),
        yaxis=dict(
            title=y_title,
            titlefont=dict(
                family='Courier New, monospace',
                size=18,
                color='#7f7f7f'
            )
        )
    )

    fig = go.Figure(data=plot_data, layout=layout)

    py.plot(fig, filename=file_name, validate=False)


def make_name(names, driver=None, meter=None, number=None):
    line = ''
    for name in names:
        if name == 'driver':
            line += driver + '/'
        elif name == 'meter':
            line += meter + '/'
        elif name == 'number':
            line += str(number) + '/'
    return line


def normalize_data(name_exp, name_databases, data_start, data_end):
    final_dict = {}
    for datab in client.list_of_database():
        db = datab['name']
        for exp in name_exp:
            if exp in db:
                if '%' in db:
                    name = db.split('.')[-1].split('%')[0]
                    number_of_records = db.split('.')[-1].split('%')[1]
                else:
                    name = db.split('.')[-1]
                    number_of_records = 'result'
                client.switch_database(db)
                measurement = client.show_measurements()[0]['name']
                data = client.query('select * from ' + measurement)
                last_driver = ''
                for point in data:
                    if point['driver'] in name_databases:
                        if point['driver'] not in final_dict:
                            final_dict[point['driver']] = {}
                        if name not in final_dict[point['driver']]:
                            final_dict[point['driver']][exp] = {}
                        if number_of_records not in final_dict[point['driver']][exp]:
                            final_dict[point['driver']][exp][number_of_records] = []
                        final_dict[point['driver']][exp][number_of_records].append(point['result'])
                        last_driver = point['driver']
                final_dict[last_driver][exp][number_of_records] = \
                    final_dict[last_driver][exp][number_of_records][data_start:data_end]
    return final_dict


generate()