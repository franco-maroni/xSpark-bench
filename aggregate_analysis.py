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
import argparse
import spark_time_analysis.cfg as ta_cfg
import json
import util.utils as utils
import shutil

AVG_GQ_PROFILED = 0.8461182608384832
DEFAULT_NUM_RECORDS = 1388043081
DEFAULT_NUM_CORES = 18
IMGS_FOLDER = 'imgs'
ESSENTIAL_FILES = ['app.json', 'app.dat', 'config.json', '*_time_analysis']

def compute_t_task(stages_struct, num_records, num_task=None):
    """
    computes t_task for all the stages and modifies stages_struct to include it.
    :param stages_struct: data structure containing the
    :param num_records: total number of input records
    :param num_task: number of tasks for each stages (currently uniform)
    """
    reads = {}
    writes = {}
    stage_id_list = [int(x) for x in stages_struct.keys()]
    stage_id_list.sort()
    for i in stage_id_list:  # range(0, len(stages_struct)):
        stage = stages_struct[str(i)]
        stage_id = str(i)
        if len(stage['parentsIds']) == 0:
            # print(stage_id)
            if not num_records:
                num_records = stage['actual_records_read']
            reads[stage_id] = num_records
        else:
            reads[stage_id] = 0
            # print(stage_id)
            for parent_id in stage['parentsIds']:
                reads[stage_id] += writes[str(parent_id)]
        writes[stage_id] = reads[stage_id] * stage['avg_io_factor']
        if not num_task:
            num_task = stage['numtask']
        stage['t_task'] = round(stage['avg_t_record'] * reads[stage_id] / (num_task * stage['avg_gq']), 4)
        stage['records_read'] = reads[stage_id]


def generate_spark_context(app_dir, app_name, num_records, num_cores, deadline, profiled_stages, avg_gq, avg_t_record, avg_io):
    stages_essential = {}
    for k, v in profiled_stages.items():
        stages_essential[k] = {}
        stages_essential[k]['id'] = v['id']
        #stages_essential[k]['name'] = v['name']
        stages_essential[k]['parentsIds'] = v['parent_ids']
        stages_essential[k]['skipped'] = v['skipped']
        stages_essential[k]['numtask'] = v['num_task']
        stages_essential[k]['avg_gq'] = avg_gq[k]
        stages_essential[k]['avg_t_record'] = avg_t_record[k]
        stages_essential[k]['avg_io_factor'] = avg_io[k]

    compute_t_task(stages_essential, num_records)

    seq_duration = 0
    for k, v in stages_essential.items():
        seq_duration += v['t_task'] * v['numtask'] / num_cores

    deadline = int(seq_duration)
    print('estimated sequential duration: {}ms'.format(deadline))

    SPARK_CONTEXT = {
        "app_name": "{}_c{}_t{}_{}l_d{}_tc_{}_n_rounds_{}".format(app_name,
                                                                  num_cores,
                                                                  ta_cfg.TIME_BOUND,
                                                                  "no_" if ta_cfg.NO_LOOPS else "",
                                                                  deadline,
                                                                  "parametric" if ta_cfg.PARAMETRIC_TC else "by20",
                                                                  "by2"),
        #        "app_dir_acceleration_0_1000_c48_t40_no-l_d133000_tc_parametric_forall_nrounds_TEST",
        "verification_params":
            {
                "plugin": ta_cfg.PLUGIN,
                "time_bound": ta_cfg.TIME_BOUND,
                "parametric_tc": ta_cfg.PARAMETRIC_TC,
                "no_loops": ta_cfg.NO_LOOPS
            },
        "tot_cores": num_cores,
        "analysis_type": "feasibility",
        "deadline": deadline,
        "max_time": deadline,
        "tolerance": ta_cfg.TOLERANCE,
        "stages": stages_essential
    }



    out_path_context = os.path.join(app_dir, 'test_context.json')
    print("dumping to {}".format(out_path_context))
    with open(out_path_context, 'w') as outfile:
        json.dump(SPARK_CONTEXT, outfile, indent=4, sort_keys=True)


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
    url = py.plot(fig, filename=title, auto_open=False)
    fig = py.get_figure(url)
    local_path = os.path.abspath(os.path.join(IMGS_FOLDER, '{}.png'.format(fig['layout']['title'])))
    print("{} -> local: {}".format(url, local_path))
    py.image.save_as(fig, local_path)
    # offline.plot(figure_or_data=fig, filename=title+'.html', image='png', image_filename=title)


def generate_plots(res, stages_keys, input_dir):
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
        trace_list.append(get_scatter(x_axis, res, 'avg_total_percentile' + str(p)))

    data_exec_times = go.Data(trace_list)

    trace_avg_GQ_master = get_scatter(x_axis, res, 'avg_GQ_master')
    trace_avg_GQ_executor = get_scatter(x_axis, res, 'avg_GQ_executor')
    data_gq = go.Data([trace_avg_GQ_master, trace_avg_GQ_executor])
    '''
    plot_figure(data=data_gq,
                title='pagerank_GQ_' + input_dir.strip('/').split('/')[-1],
                x_axis_label="Num Vertices",
                y_axis_label="GQ Value")
    '''
    trace_list_avg_gq = []
    trace_list_std_gq = []
    trace_list_avg_t_record = []
    trace_list_std_t_record = []
    for k in stages_keys:
        trace_list_avg_gq.append(get_scatter(x_axis, res, 'avg_GQ_S' + str(k)))
        trace_list_avg_gq.append(get_scatter(x_axis, res, 'std_GQ_S' + str(k)))
        trace_list_avg_t_record.append(get_scatter(x_axis, res, 'avg_t_record_S' + str(k)))
        trace_list_std_t_record.append(get_scatter(x_axis, res, 'std_t_record_S' + str(k)))

    data_gq_stages = go.Data(trace_list_avg_gq)
    data_t_record_stages = go.Data(trace_list_avg_t_record)

    plot_figure(data=data_gq_stages,
                title='average_GQ_' + input_dir.strip('/').split('/')[-1],
                x_axis_label="Num Vertices",
                y_axis_label='Value ([0, 1])')

    plot_figure(data=data_t_record_stages,
                title='average_record_time_' + input_dir.strip('/').split('/')[-1],
                x_axis_label="Num Vertices",
                y_axis_label='Time (ms)')

    plot_figure(data=data_exec_times,
                title='pagerank_execution_times_' + input_dir.strip('/').split('/')[-1],
                x_axis_label="Num Vertices",
                y_axis_label='Time (ms)')


def extract_essential_files(args):
    input_dir = args.exp_dir
    analysis_files_dir = '{}_time_analysis'.format(input_dir)
    utils.make_sure_path_exists(analysis_files_dir)
    for d in glob.glob(os.path.join(input_dir, 'app-*')):
        dest_dir = os.path.join(analysis_files_dir, d.split(os.sep)[-1])
        utils.make_sure_path_exists(dest_dir)
        for f in ESSENTIAL_FILES:
            for x in glob.glob(os.path.join(d, f)):
                shutil.copy(x, dest_dir)


def time_analysis(args):
    input_dir = args.exp_dir
    user_num_records = args.num_records if args.num_records else DEFAULT_NUM_RECORDS
    user_num_cores = args.num_cores if args.num_cores else DEFAULT_NUM_CORES
    user_deadline = args.deadline
    plot = args.plot
    analysis_id = input_dir.strip('/').split('/')[-1]
    reprocess = args.reprocess
    executors = args.executors

    gq = gq_avg= {}
    t_records_s = t_record_avg = {}
    io_factor = io_factor_avg = {}
    stages_sample = job_sample = None
    exp_report = {}
    for d in glob.glob(os.path.join(input_dir, 'app-*')):
        if executors:
            run_ta.modify_executors(d, executors)
        if reprocess:  # run time_analysis on d
            ta_job, ta_stages = run_ta.main(d)
        else:  # get precomputed analysis file from d
            ta_file_paths = glob.glob(os.path.join(d, '*_time_analysis.json'))
            if ta_file_paths:
                print("getting time_analysis from {}...".format(ta_file_paths[0]))
                with open(ta_file_paths[0]) as ta_file:
                    ta_total = json.load(ta_file)
                    ta_job = ta_total['job']
                    ta_stages = ta_total['stages']
            else:
                ta_job, ta_stages = run_ta.main(d)
        num_v = ta_job['num_v'][1]
        if not stages_sample:
            stages_sample = ta_stages
        if not gq:
            gq = gq_avg = {k: {} for k in ta_stages.keys()}
        if not t_records_s:
            t_records_s = t_record_avg = {k: {} for k in ta_stages.keys()}
        if not io_factor:
            io_factor = io_factor_avg = {k: {} for k in ta_stages.keys()}

        if num_v in exp_report:
            exp_report[num_v]['job_duration'].append(ta_job['actual_job_duration'])
            exp_report[num_v]['total_ta_executor_stages'].append(ta_job['total_ta_executor_stages'])
            exp_report[num_v]['total_ta_master_stages'].append(ta_job['total_ta_master_stages'])
            exp_report[num_v]['total_mean_plus_stddev_stages'].append(ta_job['total_mean_plus_stddev_stages'])
            for p in run_ta.PERCENTILES:
                exp_report[num_v]['total_percentile'+str(p)].append(ta_job['total_percentile'+str(p)])
            exp_report[num_v]['GQ_master'].append(ta_job['GQ_master'])
            exp_report[num_v]['GQ_executor'].append(ta_job['GQ_executor'])
            exp_report[num_v]['master_divided_by_avg_gq_profiled'].append(ta_job['total_ta_master_stages'] / AVG_GQ_PROFILED)
            for k in ta_stages.keys():
                exp_report[num_v]['t_record_S'+str(k)].append(ta_stages[k]['t_record_ta_master'])
                exp_report[num_v]['GQ_S'+str(k)].append(ta_stages[k]['s_GQ_ta_master'])
                t_records_s[k][num_v].append(ta_stages[k]['t_record_ta_master'])
                gq[k][num_v].append(ta_stages[k]['s_GQ_ta_master'])
                io_factor[k][num_v].append(ta_stages[k]['io_factor'])
        else:
            exp_report[num_v] = {}
            exp_report[num_v]['job_duration'] = [ta_job['actual_job_duration']]
            exp_report[num_v]['total_ta_executor_stages'] = [ta_job['total_ta_executor_stages']]
            exp_report[num_v]['total_ta_master_stages'] = [ta_job['total_ta_master_stages']]
            exp_report[num_v]['total_mean_plus_stddev_stages'] = [ta_job['total_mean_plus_stddev_stages']]
            for p in run_ta.PERCENTILES:
                exp_report[num_v]['total_percentile'+str(p)] = [ta_job['total_percentile'+str(p)]]
            exp_report[num_v]['GQ_master'] = [ta_job['GQ_master']]
            exp_report[num_v]['GQ_executor'] = [ta_job['GQ_executor']]
            exp_report[num_v]['master_divided_by_avg_gq_profiled'] = [ta_job['total_ta_master_stages'] / AVG_GQ_PROFILED]
            for k in ta_stages.keys():
                exp_report[num_v]['t_record_S'+str(k)] = [ta_stages[k]['t_record_ta_master']]
                exp_report[num_v]['GQ_S'+str(k)] = [ta_stages[k]['s_GQ_ta_master']]
                t_records_s[k][num_v] = [ta_stages[k]['t_record_ta_master']]
                gq[k][num_v] = [ta_stages[k]['s_GQ_ta_master']]
                io_factor[k][num_v] = [ta_stages[k]['io_factor']]

    res = {}
    # compute average of all the statistics
    for k, v in exp_report.items():
        # (k, v) -> (num_v, stat)
        res[k] = {}
        for j in v.keys():
            res[k]['avg_' + str(j)] = np.mean(v[j])
            res[k]['std_' + str(j)] = np.std(v[j])

    # compute average per stage for context generation
    for k in ta_stages.keys():
        gq_avg[k] = np.average([np.average(v) for k,v in gq[k].items()])
        t_record_avg[k] = np.average([np.average(v) for k,v in t_records_s[k].items()])
        io_factor_avg[k] = np.average([np.average(v) for k,v in io_factor[k].items()])

    print("avg_gq: {}".format(gq_avg))
    print("avg_t_record: {}".format(t_record_avg))
    print("avg_io_factor: {}".format(io_factor_avg))
    generate_spark_context(app_dir=input_dir, app_name=analysis_id, num_records=user_num_records,
                           num_cores=user_num_cores, deadline=user_deadline,
                           profiled_stages=stages_sample, avg_gq=gq_avg, avg_io=io_factor_avg,
                           avg_t_record=t_record_avg)

    if plot:
        generate_plots(res, ta_stages.keys(), input_dir)
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


def pro_runner(args):
    reprocess = args.reprocess
    exp_dir = args.exp_dir
    for d in glob.glob(os.path.join(exp_dir, 'app-*')):
        profiling.main(input_dir=d, json_out_dir=d, reprocess=reprocess)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=
            """
            Aggregated Analysis Tool for xSparkBench
            """
    )

    subparsers = parser.add_subparsers()
    parser_pro = subparsers.add_parser('pro', help='launch profiling on selected folders')
    parser_ta = subparsers.add_parser('ta', help='launch time_analysis on selected_folder')
    parser_ee = subparsers.add_parser('ee', help='extract essential files to carry on further analysis '
                                                 '({})'.format(ESSENTIAL_FILES))

    parser_pro.add_argument("exp_dir", help="directory containing all the experiment files to be analyzed")
    parser_pro.add_argument("-r", "--reprocess", dest="reprocess", action="store_true",
                            help="reprocess data (look for logs in processed_logs folders)"
                                 "[default: %(default)s]")

    parser_ta.add_argument("exp_dir", help="directory containing all the experiment files to be analyzed")
    parser_ta.add_argument("-p", "--plot", dest="plot", action="store_true",
                           help="plots the performed analyses"
                                 "[default: %(default)s]")
    parser_ta.add_argument("-i", "--input_num_records", dest="num_records", type=int,
                           help="number of input_records to be considered for the generated json context"
                                 "[default: %(default)s]")

    parser_ta.add_argument("-c", "--num_cores", dest="num_cores", type=int,
                           help="number of cores to be considered for the generated json context"
                                 "[default: %(default)s]")
    parser_ta.add_argument("-r", "--reprocess", dest="reprocess", action="store_true",
                           help="reprocess data (look for logs in provided folders)"
                                "[default: %(default)s]")

    parser_ta.add_argument("-d", "--deadline", dest="deadline", type=int,
                            help="deadline to be considered in json context generation"
                                 "[default: %(default)s]")
    parser_ta.add_argument("-e", "--executors", dest="executors", type=int,
                           help="executors"
                                "[default: %(default)s]")
    parser_ee.add_argument("exp_dir", help="directory containing all the experiment files to be extracted")

    parser_ta.add_argument("-e", "--executors", dest="executors", type=int,
                           help="executors"
                                "[default: %(default)s]")

    parser_pro.set_defaults(func=pro_runner)
    parser_ta.set_defaults(func=time_analysis)
    parser_ee.set_defaults(func=extract_essential_files)

    args = parser.parse_args()

    try:
        getattr(args, "func")
    except AttributeError:
        parser.print_help()
        sys.exit(0)

    args.func(args)
