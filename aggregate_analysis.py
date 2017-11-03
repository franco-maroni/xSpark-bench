from spark_time_analysis import run as run_ta
from spark_log_profiling import processing as profiling

import glob
import os
import sys
import plotly.plotly as py
import plotly.graph_objs as go
from functools import reduce
import numpy as np
import plotly.offline as offline

AVG_GQ_PROFILED = 0.8761182608384832




def get_scatter(x_axis, res_struct, field1, field2=None):
    return go.Scatter(
                x=x_axis,
                y=[res_struct[z][field1] + res_struct[z][field2] for z in x_axis] if field2
                else [res_struct[z][field1] for z in x_axis],
                name=field1+'_+_'+field2 if field2 else field1
            )

def get_layout(title, x_title, y_title):
    return go.Layout(
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
            ),
            exponentformat='none'
        )
    )


def plot_figure(data, title, x_axis_label, y_axis_label):
    layout = get_layout(title,
                        x_axis_label,
                        y_axis_label)
    fig = go.Figure(data=data, layout=layout)
    py.plot(fig, filename=title)
    #offline.plot(figure_or_data=fig, filename=title+'.html', image='png', image_filename=title)


def time_analysis(input_dir):
    exp_report = {}
    for dir in glob.glob(os.path.join(input_dir, 'app-*')):
        job, stages = run_ta.main(dir)
        num_v = job['num_v'][1]
        if num_v in exp_report:
            exp_report[num_v]['job_duration'].append(job['actual_job_duration'])
            exp_report[num_v]['total_ta_executor_stages'].append(job['total_ta_executor_stages'])
            exp_report[num_v]['total_ta_master_stages'].append(job['total_ta_master_stages'])
            exp_report[num_v]['total_mean_plus_stddev_stages'].append(job['total_mean_plus_stddev_stages'])
            for p in run_ta.PERCENTILES:
                exp_report[num_v]['total_percentile'+str(p)].append(job['total_percentile'+str(p)])
            exp_report[num_v]['GQ_master'].append(job['GQ_master'])
            exp_report[num_v]['GQ_executor'].append(job['GQ_executor'])
            exp_report[num_v]['master_divided_by_avg_gq_profiled'].append(job['total_ta_master_stages'] / AVG_GQ_PROFILED)
            for k in stages.keys():
                exp_report[num_v]['t_record_S'+str(k)].append(stages[k]['record_processing_time'])
                exp_report[num_v]['GQ_S'+str(k)].append(stages[k]['s_GQ'])
        else:
            exp_report[num_v] = {}
            exp_report[num_v]['job_duration'] = [job['actual_job_duration']]
            exp_report[num_v]['total_ta_executor_stages'] = [job['total_ta_executor_stages']]
            exp_report[num_v]['total_ta_master_stages'] = [job['total_ta_master_stages']]
            exp_report[num_v]['total_mean_plus_stddev_stages'] = [job['total_mean_plus_stddev_stages']]
            for p in run_ta.PERCENTILES:
                exp_report[num_v]['total_percentile'+str(p)] = [job['total_percentile'+str(p)]]
            exp_report[num_v]['GQ_master'] = [job['GQ_master']]
            exp_report[num_v]['GQ_executor'] = [job['GQ_executor']]
            exp_report[num_v]['master_divided_by_avg_gq_profiled'] = [job['total_ta_master_stages'] / AVG_GQ_PROFILED]
            for k in stages.keys():
                exp_report[num_v]['t_record_S'+str(k)] = [stages[k]['record_processing_time']]
                exp_report[num_v]['GQ_S'+str(k)] = [stages[k]['s_GQ']]

    res = {}
    # compute average of all the statistics
    for k, v in exp_report.items():
        # print('{} {}'.format(k, v))
        res[k] = {}
        for j in v.keys():
            res[k]['avg_' + str(j)] = reduce(lambda x, y: x + y, v[j]) / len(v[j])
            res[k]['std_' + str(j)] = np.std(v[j])

    x_axis = list(res.keys())
    x_axis.sort()
    trace_avg_job_duration = get_scatter(x_axis, res, 'avg_job_duration')
    trace_avg_total_ta_executor_stages = get_scatter(x_axis, res, 'avg_total_ta_executor_stages')
    trace_avg_total_ta_master_stages = get_scatter(x_axis, res, 'avg_total_ta_master_stages')
    trace_avg_total_mean_plus_stddev_stages = get_scatter(x_axis, res, 'avg_total_mean_plus_stddev_stages')
    trace_master_divided_by_avg_gq_profiled = get_scatter(x_axis, res, 'avg_master_divided_by_avg_gq_profiled')

    trace_list = [trace_avg_job_duration,
                  trace_avg_total_ta_executor_stages,
                  trace_avg_total_ta_master_stages,
                  trace_avg_total_mean_plus_stddev_stages,
                  trace_master_divided_by_avg_gq_profiled]
    for p in run_ta.PERCENTILES:
        trace_list.append(get_scatter(x_axis, res, 'avg_total_percentile'+str(p)))

    data_exec_times = go.Data(trace_list)

    trace_avg_GQ_master = get_scatter(x_axis, res, 'avg_GQ_master')
    trace_avg_GQ_executor = get_scatter(x_axis, res, 'avg_GQ_executor')
    data_gq = go.Data([trace_avg_GQ_master, trace_avg_GQ_executor])

    plot_figure(data=data_gq,
                title='pagerank_GQ_' + input_dir.strip('/').split('/')[-1],
                x_axis_label="Num Vertices",
                y_axis_label="GQ Value")

    trace_list_avg_gq = []
    trace_list_std_gq = []
    trace_list_avg_t_record = []
    trace_list_std_t_record = []
    for k in stages.keys():
        trace_list_avg_gq.append(get_scatter(x_axis, res, 'avg_GQ_S' + str(k)))
        trace_list_std_gq.append(get_scatter(x_axis, res, 'std_GQ_S' + str(k)))
        trace_list_avg_t_record.append(get_scatter(x_axis, res, 'avg_t_record_S' + str(k)))
        trace_list_std_t_record.append(get_scatter(x_axis, res, 'std_t_record_S' + str(k)))

    data_gq_stages = go.Data(trace_list_avg_gq)
    data_t_record_stages = go.Data(trace_list_avg_t_record)

    plot_figure(data=data_gq_stages,
                title='average_GQ_' + input_dir.strip('/').split('/')[-1],
                x_axis_label="Num Vertices",
                y_axis_label='Time (ms)')

    plot_figure(data=data_t_record_stages,
                title='average_record_time_' + input_dir.strip('/').split('/')[-1],
                x_axis_label="Num Vertices",
                y_axis_label='Time (ms)')


    """
        zipped_job_d_master = zip(trace_avg_job_duration.get('y'), trace_avg_total_ta_master_stages.get('y'))
        trace_percents_total_master = go.Scatter(
            x=x_axis,
            y=[(x-y)/y*100 for x,y in zipped_job_d_master],
            name='total_master_perc'
        )
        data_perc = go.Data([trace_percents_total_master])

        py.plot(data_perc, filename='pagerank_total_master_perc_'+input_dir.strip('/').split('/')[-1])
    """
    plot_figure(data=data_exec_times,
                title='pagerank_execution_times_' + input_dir.strip('/').split('/')[-1],
                x_axis_label="Num Vertices",
                y_axis_label='Time (ms)')


if __name__ == "__main__":
    if len(sys.argv) > 2:
        app_type = sys.argv[1]
        in_dir = os.path.abspath(sys.argv[2])
        if app_type == 'ta':
            time_analysis(input_dir=in_dir)
        elif app_type == 'pro':
            for dir in glob.glob(os.path.join(in_dir, 'app-*')):
                profiling.main(input_dir=dir, json_out_dir=dir)
        else:
            print('Invalid app_type! allowed values: ta | pro), inserted value: {}', app_type)
    else:
        print('ERROR: You must provide at least two arguments')
        sys.exit(0)
