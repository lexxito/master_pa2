import json

import plotly.plotly as py
import plotly.graph_objs as go
import database

from scipy import stats


py.sign_in('aleksey.sergien', 'WRBJNVEVssmXHxrKRuSS')
client = database.Client()

exp_names_DEFAULT = ['writing_multiple_databases']
name_of_measurements_DEFAULT = ['write_records']
colors_DEFAULT = {'influxdb': {'full': '#d7724a', 'line': '#a73508'},
                  'opentsdb': {'full': '#6fe314', 'line': '#327300'},
                  'postgresdb': {'full': '#226ad9', 'line': '#002d73'}}
order_DEFAULT = ['influxdb', 'postgresdb', 'opentsdb']
downsample_size_DEFAULT = 100
title_DEFAULT = 'Records writing'
x_title_DEFAULT = 'number of records'
y_title_DEFAULT = 'speed of writing, records per second'
file_name_DEFAULT = 'writing records'


def generate(exp_names=exp_names_DEFAULT, name_of_measurements=name_of_measurements_DEFAULT,
             colors=colors_DEFAULT, order=order_DEFAULT, downsample_size=downsample_size_DEFAULT, title=title_DEFAULT,
             x_title=x_title_DEFAULT, y_title=y_title_DEFAULT, file_name=file_name_DEFAULT):
    plot_data = []
    data = []
    for datab in client.list_of_database():
        db = datab['name']
        for exp_name in exp_names:
            if exp_name in db:
                client.switch_database(db)
                for measurement in name_of_measurements:
                    query = 'select * from ' + measurement
                    data += client.query(query)

    overall_data = downsample(downsample_size, normalize_data(data))

    for driver in order:
        plot_data.append(
            go.Scatter(
                x=overall_data[driver]['x'],
                y=overall_data[driver]['y'],
                mode='markers',
                marker=dict(
                    color=colors[driver]['full']
                ),
                name=driver
            )
        )
        plot_data.append(
            go.Scatter(
                x=overall_data[driver]['x'],
                y=line(overall_data[driver]['x'], overall_data[driver]['y']),
                line=dict(
                    width=5,
                    color=colors[driver]['line']
                ),
                showlegend=False,
                mode='lines'
            )
        )

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

    py.plot(fig, filename=file_name)


def line(x, y):
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    line_y = []
    #print(slope, intercept)
    for x_element in x:
        line_y.append(slope*x_element+intercept)
    return line_y


def normalize_data(data):
    overall_data = {}
    for point in data:
        if point['driver'] not in overall_data:
            overall_data[point['driver']] = {'y': [], 'x': []}
        records = 0

        met = str(point['metadata']).replace("\'", "\"")

        if 'records' in json.loads(met):
            records += json.loads(met)['records']

        result = float(point['result'])

        overall_data[point['driver']]['y'].append(records/result)

        if not overall_data[point['driver']]['x']:
            overall_data[point['driver']]['x'].append(records)
        else:
            overall_data[point['driver']]['x'].append(overall_data[point['driver']]['x'][-1] + records)
    return overall_data


def downsample(size, data):
    new_dict = {}
    for key in data:
        new_dict[key] = {'y': [], 'x': []}
        i = 0
        overall_index = 0
        down_x = 0
        down_y = 0
        while True:
            down_x += data[key]['x'][overall_index]
            down_y += data[key]['y'][overall_index]
            overall_index += 1
            i += 1
            if i == size :
                new_dict[key]['x'].append(down_x)
                new_dict[key]['y'].append(down_y/(i+1))
                i = 0
                down_x = 0
                down_y = 0
            if overall_index == len(data[key]['x']):
                new_dict[key]['x'].append(down_x)
                new_dict[key]['y'].append(down_y/(i+1))
                break
    return new_dict
