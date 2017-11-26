from spark_time_analysis import run as run_ta
from spark_log_profiling import processing as profiling

import glob
import os
import sys
import plotly.plotly as py
import plotly.graph_objs as go
from functools import reduce
import numpy as np
import argparse
import spark_time_analysis.cfg as ta_cfg
import json
import util.utils as utils
import shutil
import pprint
import collections
import math
import config
from util.ssh_client import CustomSSHClient
from plumbum.machines.paramiko_machine import ParamikoMachine
from plumbum import BG
from util.plot_analyses import get_scatter, get_scatter2, get_layout, plot_figure

D_VERT_SERVER_HOSTNAME = '40.84.157.107'
BASE_JSON2MC_PATH = '/home/ubuntu/DICE-Verification/d-vert-server/d-vert-json2mc/'

DEFAULT_NUM_RECORDS = 200000000
DEFAULT_NUM_CORES = 16
IMGS_FOLDER = 'imgs_local'

ESSENTIAL_FILES = ['app.json', 'app.dat', 'config.json', '*_time_analysis.json']
JOB_STATS = ['actual_job_duration', 'total_ta_executor_stages', 'total_ta_master_stages', 'total_overhead_monocore',
             'GQ_master'] + ['total_percentile' + str(p) for p in run_ta.PERCENTILES]
STAGES_STATS = ['io_factor', 't_record_ta_master', 's_GQ_ta_master', 's_avg_duration_ta_master']

JOB_STATS_BIG_JSON = ['actual_job_duration', 'num_v', 'num_cores']
STAGES_STATS_BIG_JSON = ['add_to_end_taskset', 'actual_records_read', 's_GQ_ta_master', 's_GQ_ta_executor',
                         't_record_ta_executor', 't_record_ta_master', 'io_factor', 't_task_ta_master',
                         'task_durations']

SIMPLE_AVERAGE_STATS = ['avg_actual_job_duration',
                        'avg_total_ta_executor_stages',
                        'avg_total_ta_master_stages'] + ['avg_total_percentile' + str(p) for p in run_ta.PERCENTILES]
COMBINED_STATS = ['avg_total_with_avg_gq_and_ta_master',
                  'avg_total_with_avg_gq_and_ta_executor',
                  'avg_total_with_avg_gq_and_ta_executor_plus_overhead',
                  'avg_total_with_local_gq_and_ta_master',
                  'avg_total_with_avg_t_task_master',
                  'avg_total_with_local_t_task_master']

PLOT_EXEC_TIMES_STATS = SIMPLE_AVERAGE_STATS + COMBINED_STATS


def compute_t_task(stages_struct, num_records, num_task=None):
    """
    computes t_task for all the stages and modifies stages_struct to include it.
    :param stages_struct: data structure containing the
    :param num_records: total number of input records
    :param num_task: number of tasks for each stages (currently uniform)
    :returns t_tasks dictionary, t_tasks_num_v dictionary, num_tasks dictionary
    """
    reads = {}
    writes = {}
    stage_id_list = [int(x) for x in stages_struct.keys()]
    stage_id_list.sort()
    for i in stage_id_list:
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
        stage['records_read'] = reads[stage_id]
        # compute t_task with avg_t_record and avg_gq
        stage['t_task'] = stage['avg_t_record'] * reads[stage_id] / (num_task * stage['avg_gq'])
        num_v = str(int(num_records / 20))
        print("looking for {} in {}".format(num_v, stage['avg_t_record_num_v']))
        if num_v in stage['avg_t_record_num_v']:
            # compute t_task with "local" avg_t_record_num_v and avg_gq
            stage['t_task_num_v'] = stage['avg_t_record_num_v'][num_v] * reads[stage_id] / (
                num_task * stage['avg_gq'])
        else:
            stage['t_task_num_v'] = 0
    return {s['id']: s['t_task'] for s in stages_struct.values()}, \
           {s['id']: s['t_task_num_v'] for s in stages_struct.values()}, \
           {s['id']: num_task for s in stages_struct.values()}


def build_generic_stages_struct(profiled_stages, avg_gq, avg_t_record, avg_io, avg_gq_num_v, avg_t_record_num_v):
    generic_stages_struct = {}
    for k, v in profiled_stages.items():
        generic_stages_struct[k] = {}
        generic_stages_struct[k]['id'] = v['id']
        # generic_stages_struct[k]['name'] = v['name']
        generic_stages_struct[k]['parentsIds'] = v['parent_ids']
        generic_stages_struct[k]['skipped'] = v['skipped']
        generic_stages_struct[k]['numtask'] = v['num_task']
        generic_stages_struct[k]['avg_gq'] = avg_gq[k]
        generic_stages_struct[k]['avg_gq_num_v'] = avg_gq_num_v[k]
        generic_stages_struct[k]['avg_t_record'] = avg_t_record[k]
        generic_stages_struct[k]['avg_t_record_num_v'] = avg_t_record_num_v[k]
        generic_stages_struct[k]['avg_io_factor'] = avg_io[k]
    return generic_stages_struct

def build_generic_stages_struct2(profiled_stages, res):  # avg_gq, avg_t_record, avg_io, avg_gq_num_v, avg_t_record_num_v):
    generic_stages_struct = {}
    for k, v in profiled_stages.items():
        generic_stages_struct[k] = {}
        generic_stages_struct[k]['id'] = v['id']
        # generic_stages_struct[k]['name'] = v['name']
        generic_stages_struct[k]['parentsIds'] = v['parent_ids']
        generic_stages_struct[k]['skipped'] = v['skipped']
        generic_stages_struct[k]['numtask'] = v['num_task']
        generic_stages_struct[k]['avg_gq'] = np.mean(list(res['avg_s_GQ_ta_master'][k].values()))  #avg_gq[k]
        print(res['avg_s_GQ_ta_master'][k])
        generic_stages_struct[k]['avg_gq_num_v'] = res['avg_s_GQ_ta_master'][k]  # avg_gq_num_v[k]
        print(list(res['avg_t_record_ta_master'][k].values()))
        generic_stages_struct[k]['avg_t_record'] = np.mean(list(res['avg_t_record_ta_master'][k].values()))  #avg_t_record[k]
        generic_stages_struct[k]['avg_t_record_num_v'] = res['avg_t_record_ta_master'][k]  # avg_t_record_num_v[k]
        print(list(res['avg_io_factor'][k].values()))
        generic_stages_struct[k]['avg_io_factor'] = np.mean(list(res['avg_io_factor'][k].values()))  # avg_io[k]
    return generic_stages_struct


def generate_spark_context(args):
    exp_dir = os.path.abspath(args.exp_dir)
    run_verification = args.verify
    analysis_id = exp_dir.strip('/').split('/')[-1]
    num_records = args.num_records if args.num_records else DEFAULT_NUM_RECORDS
    num_cores = args.num_cores if args.num_cores else DEFAULT_NUM_CORES
    deadlines = args.deadlines
    num_tasks = args.num_tasks
    generic_stages_path = glob.glob(os.path.join(exp_dir, '{}_generic_stages.json'.format(analysis_id)))
    if not generic_stages_path:
        print('{}_generic_stages.json FILE NOT FOUND!\nRUN PROFILING/TIME_ANALYSIS FIRST'.format(analysis_id))
        sys.exit(1)
    else:
        with open(generic_stages_path[0]) as gsf:
            generic_stages_struct = json.load(gsf)
    compute_t_task(generic_stages_struct, num_records, num_tasks)

    seq_duration = seq_duration_num_v = 0
    for k, v in generic_stages_struct.items():
        if not num_tasks:
            num_tasks = v['numtask']
        else:
            v['numtask'] = num_tasks
        seq_duration_num_v += v['t_task_num_v'] * math.ceil(num_tasks / num_cores)
        seq_duration += v['t_task'] * math.ceil(num_tasks / num_cores)
    if not deadlines:
        deadlines = [int(seq_duration_num_v)]
    print('estimated "local" sequential duration: {}ms'.format(int(seq_duration_num_v)))
    print('estimated average sequential duration: {}ms'.format(int(seq_duration)))

    for d in deadlines:
        print('Generating JSON file for deadline {}'.format(d))
        app_name = "{}_c{}_t{}_nr{}_tb{}_{}l_d{}_tc_{}_n_rounds_{}".format(analysis_id,
                                                                      num_cores,
                                                                      num_tasks,
                                                                      num_records,
                                                                      ta_cfg.TIME_BOUND,
                                                                      "no_" if ta_cfg.NO_LOOPS else "",
                                                                      d,
                                                                      "parametric" if ta_cfg.PARAMETRIC_TC else "28_16",
                                                                      "by1")
        #        "exp_dir_acceleration_0_1000_c48_t40_no-l_d133000_tc_parametric_forall_nrounds_TEST",
        SPARK_CONTEXT = {
            "app_name": app_name,
            "verification_params":
                {
                    "plugin": ta_cfg.PLUGIN,
                    "time_bound": ta_cfg.TIME_BOUND,
                    "parametric_tc": ta_cfg.PARAMETRIC_TC,
                    "no_loops": ta_cfg.NO_LOOPS
                },
            "tot_cores": num_cores,
            "analysis_type": "feasibility",
            "deadline": d,
            "max_time": d,
            "tolerance": ta_cfg.TOLERANCE,
            "stages": generic_stages_struct
        }

        out_path_context = os.path.join(exp_dir, '{}_context.json'.format(app_name))
        print("dumping to {}".format(out_path_context))
        with open(out_path_context, 'w') as outfile:
            json.dump(SPARK_CONTEXT, outfile, indent=4, sort_keys=True)
        if run_verification:
            ssh_launch_json2mc(out_path_context)



def generate_plots(res, stages_keys, input_dir):
    x_axis = list(res.keys())
    x_axis.sort()

    trace_list = [get_scatter(x_axis, res, stat) for stat in PLOT_EXEC_TIMES_STATS]

    data_exec_times = go.Data(trace_list)

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
                y_axis_label='Value ([0, 1])',
                out_folder=IMGS_FOLDER)

    plot_figure(data=data_t_record_stages,
                title='average_record_time_' + input_dir.strip('/').split('/')[-1],
                x_axis_label="Num Vertices",
                y_axis_label='Time (ms)',
                out_folder=IMGS_FOLDER)

    plot_figure(data=data_exec_times,
                title='pagerank_execution_times_' + input_dir.strip('/').split('/')[-1],
                x_axis_label="Num Vertices",
                y_axis_label='Time (ms)',
                out_folder=IMGS_FOLDER)


def generate_plots2(res, stages_keys, input_dir, num_v_set):
    x_axis = list(num_v_set)
    x_axis.sort()
    #stats = ['avg_actual_job_duration',
    #         'avg_total_ta_executor_stages',
    #         'avg_total_ta_master_stages'#,
            # 'avg_total_with_gq_profiled_and_ta_master',
            # 'avg_total_with_gq_profiled_and_ta_executor',
            # 'avg_total_with_gq_profiled_and_ta_executor_plus_overhead',
            # 'avg_total_with_gq__and_ta_master',
            # 'avg_total_with_gq_profiled_and_tr_profiled_master',
            # 'avg_total_with_gq_and_tr_profiled_master'
     #        ]

    trace_list = [get_scatter2(x_axis, res, stat) for stat in PLOT_EXEC_TIMES_STATS]

    #for p in run_ta.PERCENTILES:
    #    trace_list.append(get_scatter2(x_axis, res, 'avg_total_percentile' + str(p)))

    data_exec_times = go.Data(trace_list)

    trace_list_avg_gq = []
    trace_list_std_gq = []
    trace_list_avg_t_record = []
    trace_list_std_t_record = []
    for k in stages_keys:
        trace_list_avg_gq.append(get_scatter2(x_axis, res['avg_s_GQ_ta_master'], str(k)))
        trace_list_avg_gq.append(get_scatter2(x_axis, res['std_s_GQ_ta_master'], str(k)))
        trace_list_avg_t_record.append(get_scatter2(x_axis, res['avg_t_record_ta_master'], str(k)))
        trace_list_avg_t_record.append(get_scatter2(x_axis, res['std_t_record_ta_master'], str(k)))

    data_gq_stages = go.Data(trace_list_avg_gq)
    data_t_record_stages = go.Data(trace_list_avg_t_record)

    plot_figure(data=data_gq_stages,
                title='average_GQ_{}_2'.format(input_dir.strip('/').split('/')[-1]),
                x_axis_label="Num Vertices",
                y_axis_label='Value ([0, 1])',
                out_folder=IMGS_FOLDER)

    plot_figure(data=data_t_record_stages,
                title='average_record_time_{}_2'.format(input_dir.strip('/').split('/')[-1]),
                x_axis_label="Num Vertices",
                y_axis_label='Time (ms)',
                out_folder=IMGS_FOLDER)

    plot_figure(data=data_exec_times,
                title='pagerank_execution_times_{}_2'.format(input_dir.strip('/').split('/')[-1]),
                x_axis_label="Num Vertices",
                y_axis_label='Time (ms)',
                out_folder=IMGS_FOLDER)

def extract_essential_files(input_dir):
    analysis_files_dir = os.path.abspath(os.path.join(os.path.dirname(input_dir.strip(os.sep)),
                                                      'ta_only',
                                                      '{}_time_analysis'.format(
                                                          input_dir.strip(os.sep).split(os.sep)[-1])))
    print('analysis_files_dir: {}'.format(analysis_files_dir))
    utils.make_sure_path_exists(analysis_files_dir)
    for d in glob.glob(os.path.join(input_dir, 'app-*')):
        dest_dir = os.path.join(analysis_files_dir, d.split(os.sep)[-1])
        utils.make_sure_path_exists(dest_dir)
        for f in ESSENTIAL_FILES:
            for x in glob.glob(os.path.join(d, f)):
                print('copying:\t{}\nto:\t{}'.format(x, dest_dir))
                shutil.copy(x, dest_dir)


def collect_all_time_analysis(exp_dir):
    input_dir = os.path.abspath(exp_dir)
    out_path = os.path.join(input_dir, '{}_allinone_stats.json'.format(input_dir.split(os.sep)[-1]))
    print("Getting time_analysis data from all the experiments in {}".format(input_dir))
    res = {"directory": input_dir, "experiments": []}
    for d in glob.glob(os.path.join(input_dir, 'app-*')):
        print(d)
        for t in glob.glob(os.path.join(d, '*_time_analysis.json')):
            with open(t) as ta_file:
                cur_ta = json.load(ta_file)
                tmp_exp_report = {'job': {}, 'stages': {}}
                tmp_exp_report['job']['id'] = os.path.basename(t)
                for x in JOB_STATS_BIG_JSON:
                    tmp_exp_report['job'][x] = cur_ta['job'][x]
                for k, v in cur_ta['stages'].items():
                    tmp_exp_report['stages'][k] = {}
                    for x in STAGES_STATS_BIG_JSON:
                        tmp_exp_report['stages'][k][x] = v[x]
                res['experiments'].append(tmp_exp_report)
    print("dumping to {}".format(out_path))
    with open(out_path, 'w') as outfile:
        json.dump(res, outfile, indent=4, sort_keys=True)


def get_empty_dict_of_dicts(keys):
    return {k: collections.defaultdict(list) for k in keys}


def time_analysis(args):
    # get command line arguments
    input_dir = args.exp_dir
    plot = args.plot
    reprocess = args.reprocess
    collect_all_ta = args.collect_all_ta
    extract_essentials = args.extract_essentials
    # executors = args.executors
    analysis_id = input_dir.strip('/').split('/')[-1]

    num_v_set = set([])
    gq = gq_avg = gq_avg_num_v = None
    t_records_s = t_record_avg = None
    io_factor = io_factor_avg = None
    stages_sample = job_sample = None
    exp_report = {}  # exp_report[NUM_V][STAGE/JOB]
    exp_report2 = {}  # exp-report2[STAGE/JOB][NUM_V]
    ta_master = ta_master_avg = None
    for x in JOB_STATS:
        exp_report2[x] = collections.defaultdict(list)
    # iterate over all the application directories included in input_dir
    for d in glob.glob(os.path.join(input_dir, 'app-*')):
        '''
        if executors:  # if specified, modify max_executor in config.json  --> to be removed
            run_ta.modify_executors(d, executors)
        '''
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
            else:  # if precomputed analysis is not available, launch time_analysis on current directory d
                ta_job, ta_stages = run_ta.main(d)
        # save numV from configuration files of current directory
        num_v = str(ta_job['num_v'][1])
        num_v_set.add(num_v)
        if not stages_sample:  # initialize all the data structures that will be used to store statistics
            for x in STAGES_STATS:
                exp_report2[x] = get_empty_dict_of_dicts(ta_stages.keys())
            stages_sample = ta_stages
            job_sample = ta_job

            gq = get_empty_dict_of_dicts(ta_stages.keys())
            gq_avg = get_empty_dict_of_dicts(ta_stages.keys())
            gq_avg_num_v = get_empty_dict_of_dicts(ta_stages.keys())
            t_records_s = get_empty_dict_of_dicts(ta_stages.keys())
            t_record_avg = get_empty_dict_of_dicts(ta_stages.keys())
            t_record_std = get_empty_dict_of_dicts(ta_stages.keys())
            t_record_std_div_avg = get_empty_dict_of_dicts(ta_stages.keys())
            t_record_avg_num_v = get_empty_dict_of_dicts(ta_stages.keys())
            io_factor = get_empty_dict_of_dicts(ta_stages.keys())
            io_factor_avg = get_empty_dict_of_dicts(ta_stages.keys())
            ta_master = get_empty_dict_of_dicts(ta_stages.keys())
            ta_master_avg = get_empty_dict_of_dicts(ta_stages.keys())
            ta_executor = get_empty_dict_of_dicts(ta_stages.keys())
            ta_executor_avg = get_empty_dict_of_dicts(ta_stages.keys())

        if num_v not in exp_report:
            exp_report[num_v] = collections.defaultdict(list)
        for x in JOB_STATS:
            exp_report2[x][num_v].append(ta_job[x])
        exp_report[num_v]['actual_job_duration'].append(ta_job['actual_job_duration'])
        exp_report[num_v]['total_ta_executor_stages'].append(ta_job['total_ta_executor_stages'])
        exp_report[num_v]['total_ta_master_stages'].append(ta_job['total_ta_master_stages'])
        for p in run_ta.PERCENTILES:
            exp_report[num_v]['total_percentile' + str(p)].append(ta_job['total_percentile' + str(p)])
        exp_report[num_v]['GQ_master'].append(ta_job['GQ_master'])
        # exp_report[num_v]['GQ_executor'].append(ta_job['GQ_executor'])
        for k in ta_stages.keys():
            exp_report[num_v]['t_record_S' + str(k)].append(ta_stages[k]['t_record_ta_master'])
            exp_report[num_v]['GQ_S' + str(k)].append(ta_stages[k]['s_GQ_ta_master'])
            for x in STAGES_STATS:
                exp_report2[x][k][num_v].append(ta_stages[k][x])
            t_records_s[k][num_v].append(ta_stages[k]['t_record_ta_master'])
            gq[k][num_v].append(ta_stages[k]['s_GQ_ta_master'])
            io_factor[k][num_v].append(ta_stages[k]['io_factor'])
            ta_master[k][num_v].append(ta_stages[k]['s_avg_duration_ta_master'])
            ta_executor[k][num_v].append(ta_stages[k]['s_avg_duration_ta_executor'])

    if collect_all_ta:
        collect_all_time_analysis(input_dir)
    if extract_essentials:
        extract_essential_files(input_dir)
    res = {}
    # compute average and standard deviation of all the statistics
    for k, v in exp_report.items():
        if k not in JOB_STATS:
            # (k, v) -> (num_v, stat)
            res[k] = {}
            for j in v.keys():
                res[k]['avg_' + str(j)] = np.mean(v[j])
                res[k]['std_' + str(j)] = np.std(v[j])

    resulting_stats = {}
    # compute average and standard deviation of all the statistics
    for k in JOB_STATS:
        resulting_stats['avg_{}'.format(k)] = {}
        resulting_stats['std_{}'.format(k)] = {}
        for v in num_v_set:
            resulting_stats['avg_{}'.format(k)][v] = np.mean(list(exp_report2[k][v]))
            resulting_stats['std_{}'.format(k)][v] = np.std(list(exp_report2[k][v]))
    for s in STAGES_STATS:
        resulting_stats['avg_{}'.format(s)] = get_empty_dict_of_dicts(ta_stages.keys())
        resulting_stats['std_{}'.format(s)] = get_empty_dict_of_dicts(ta_stages.keys())
        for k in ta_stages.keys():
            for v in num_v_set:
                resulting_stats['avg_{}'.format(s)][k][v] = np.mean(list(exp_report2[s][k][v]))
                resulting_stats['std_{}'.format(s)][k][v] = np.std(list(exp_report2[s][k][v]))

    out_path_exp_rep2 = os.path.join(input_dir, '{}_exp_report_2.json'.format(analysis_id))
    with open(out_path_exp_rep2, 'w+') as outfile:
        json.dump(exp_report2, outfile, indent=4, sort_keys=True)
    out_path_res2 = os.path.join(input_dir, '{}_stats_2.json'.format(analysis_id))
    with open(out_path_res2, 'w+') as outfile:
        json.dump(resulting_stats, outfile, indent=4, sort_keys=True)


    for k in ta_stages.keys():
        # average gq foreach stage across different num_v
        gq_avg[k] = np.average([np.average(y) for x, y in gq[k].items()])
        # average t_record foreach stage across different num_v
        t_record_avg[k] = np.average([np.average(y) for x, y in t_records_s[k].items()])
        t_record_std[k] = np.std([np.average(y) for x, y in t_records_s[k].items()])
        t_record_avg_num_v[k] = {x: np.average(y) for x, y in t_records_s[k].items()}
        t_record_std_div_avg[k] = t_record_std[k] / t_record_avg[k]
        # average io_factor foreach stage across different num_v
        io_factor_avg[k] = np.average([np.average(y) for x, y in io_factor[k].items()])
        # average gq foreach stage and foreach num_v
        gq_avg_num_v[k] = {x: np.average(y) for x, y in gq[k].items()}
        # average tage duration foreach stage and foreach num_v
        ta_master_avg[k] = {x: np.average(y) for x, y in ta_master[k].items()}
        ta_executor_avg[k] = {x: np.average(y) for x, y in ta_executor[k].items()}

    # build generic stages dict including all the average values for stats

    generic_stages_dict_old = build_generic_stages_struct(profiled_stages=stages_sample, avg_gq=gq_avg, avg_io=io_factor_avg,
                                                      avg_t_record=t_record_avg, avg_gq_num_v=gq_avg_num_v,
                                                      avg_t_record_num_v=t_record_avg_num_v)
    generic_stages_dict = build_generic_stages_struct2(profiled_stages=stages_sample, res=resulting_stats)
    out_path_generic_s = os.path.join(input_dir, '{}_generic_stages.json'.format(analysis_id))
    print("dumping generic_stages to {}".format(out_path_generic_s))
    with open(out_path_generic_s, 'w+') as outfile:
        json.dump(generic_stages_dict, outfile, indent=4, sort_keys=True)
    out_path_generic_s_old = os.path.join(input_dir, '{}_generic_stages_old.json'.format(analysis_id))
    print("dumping generic_stages to {}".format(out_path_generic_s_old))
    with open(out_path_generic_s_old, 'w+') as outfile:
        json.dump(generic_stages_dict_old, outfile, indent=4, sort_keys=True)

    #  build estimates with different combinations
    t_tasks = {}
    t_tasks_num_v = {}
    num_tasks = {}
    print("num_v_set: {}\nnum_cores: {}".format(num_v_set, job_sample['num_cores']))
    for x in COMBINED_STATS:
        resulting_stats[x] = {}
        for v in num_v_set:
            resulting_stats[x][v] = 0
    for v in num_v_set:
        res[v]['avg_total_with_avg_gq_and_ta_master'] = 0
        res[v]['avg_total_with_avg_gq_and_ta_executor'] = 0
        res[v]['avg_total_with_avg_gq_and_ta_executor_plus_overhead'] = \
            resulting_stats['avg_total_overhead_monocore'][v]/job_sample['num_cores']
        res[v]['avg_total_with_local_gq_and_ta_master'] = 0
        res[v]['avg_total_with_avg_t_task_master'] = 0
        res[v]['avg_total_with_local_t_task_master'] = 0
        t_tasks[v], t_tasks_num_v[v], num_tasks[v] = compute_t_task(generic_stages_dict, int(v) * 20)
        for s in ta_stages.keys():
            res[v]['avg_total_with_avg_gq_and_ta_master'] += ta_master_avg[s][v] / gq_avg[s]
            res[v]['avg_total_with_avg_gq_and_ta_executor'] += ta_executor_avg[s][v] / gq_avg[s]
            res[v]['avg_total_with_local_gq_and_ta_master'] += ta_master_avg[s][v] / gq_avg_num_v[s][v]
            res[v]['avg_total_with_avg_t_task_master'] += t_tasks[v][s] * math.ceil(
                num_tasks[v][s] / job_sample['num_cores'])
            res[v]['avg_total_with_local_t_task_master'] += t_tasks_num_v[v][s] * math.ceil(
                num_tasks[v][s] / job_sample['num_cores'])
        res[v]['avg_total_with_avg_gq_and_ta_executor_plus_overhead'] = \
            res[v]['avg_total_with_avg_gq_and_ta_executor'] + \
            resulting_stats['avg_total_overhead_monocore'][v] / job_sample['num_cores']
        '''
        VERSIONE 2.0
        '''
        #t_tasks[v], t_tasks_num_v[v], num_tasks[v] = compute_t_task(generic_stages_dict, int(v) * 20)
        for s in ta_stages.keys():
            resulting_stats['avg_total_with_avg_gq_and_ta_master'][v] += ta_master_avg[s][v] / gq_avg[s]
            resulting_stats['avg_total_with_avg_gq_and_ta_executor'][v] += ta_executor_avg[s][v] / gq_avg[s]
            resulting_stats['avg_total_with_local_gq_and_ta_master'][v] += ta_master_avg[s][v] / gq_avg_num_v[s][v]
            resulting_stats['avg_total_with_avg_t_task_master'][v] += t_tasks[v][s] * math.ceil(
                num_tasks[v][s] / job_sample['num_cores'])
            resulting_stats['avg_total_with_local_t_task_master'][v] += t_tasks_num_v[v][s] * math.ceil(
                num_tasks[v][s] / job_sample['num_cores'])
        resulting_stats['avg_total_with_avg_gq_and_ta_executor_plus_overhead'][v] = \
            resulting_stats['avg_total_with_avg_gq_and_ta_executor'][v] + \
            resulting_stats['avg_total_overhead_monocore'][v] / job_sample['num_cores']


    pp = pprint.PrettyPrinter(indent=4)
    '''
    print("\navg_gq: {}".format(gq_avg))
    print("\ngq_avg_num_v: {}".format(gq_avg_num_v))
    print("\navg_t_record: {}".format(t_record_avg))
    print("\navg_io_factor: {}".format(io_factor_avg))
    print("\nt_record_avg_num_v:")
    pp.pprint(t_record_avg_num_v)
    print("\nt_record_std: {}")
    pp.pprint(t_record_std)
    print("\nt_record_std_div_avg: {}")
    pp.pprint(t_record_std_div_avg)
    '''
    # pp.pprint(exp_report2)
    if plot:
        generate_plots(res, ta_stages.keys(), input_dir)
        generate_plots2(resulting_stats, ta_stages.keys(), input_dir, num_v_set)


def pro_runner(args):
    reprocess = args.reprocess
    exp_dir = args.exp_dir
    for d in glob.glob(os.path.join(exp_dir, 'app-*')):
        profiling.main(input_dir=d, json_out_dir=d, reprocess=reprocess)


def ssh_conn(args):
    """
    apparently is not possible to run json2mc in background with only paramiko
    :param args:
    :return:
    """
    filepath = args.file_path
    client = CustomSSHClient(hostname=D_VERT_SERVER_HOSTNAME,
                             port=22,
                             username='ubuntu',
                             password=None,
                             key_files=config.PRIVATE_KEY_PATH)
    client.connect()
    destination_path = os.path.join(BASE_JSON2MC_PATH, 'd4s', os.path.basename(filepath))
    client.put(localpath=filepath,
               remotepath=destination_path)
    client.run('. {}/venv/bin/activate'.format(BASE_JSON2MC_PATH))
    status, std_out, std_err = client.run('cd {} &&  ./run_json2mc.py -T spark --db -c {} \&'.format(BASE_JSON2MC_PATH, destination_path))
    print('std_err: {}\nstd_out {}\nstatus {}'.format(std_err, std_out, status))


def ssh_launch_json2mc(filepath):
    """
    simple method that uploads the file whose path is provided as argument filepath
    and remotely launches a verification task in background
    :param filepath: path of the .json which has to be uploaded on the server and provided as a parameter to json2mc.py
    """
    print('ssh_launch_json2mc({})'.format(filepath))
    destination_path = os.path.join(BASE_JSON2MC_PATH, 'd4s', os.path.basename(filepath))
    print('connecting to {}'.format(D_VERT_SERVER_HOSTNAME))
    rem = ParamikoMachine(host=D_VERT_SERVER_HOSTNAME, keyfile=config.PRIVATE_KEY_PATH, user='ubuntu')
    print('uploading\n{}\nto\n{}:{}'.format(filepath, D_VERT_SERVER_HOSTNAME, destination_path))
    rem.upload(filepath, destination_path)
    with rem.cwd(BASE_JSON2MC_PATH):
        activate_venv = rem['./activate_venv.sh']
        run_json2mc = rem['./run_json2mc.py']
        print('activating venv...')
        activate_venv()
        print('launching json2mc...')
        f = run_json2mc['-T', 'spark', '--db', '-c', destination_path] & BG
        if f.ready():
            print('Command exited with return_code {}\nSTDOUT:{}\nSTDERR:{}'.format(f.returncode, f.stdout, f.stderr))
        else:
            print('Command running in background...')



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
    parser_gen = subparsers.add_parser('gen', help='generate json file for formal analysis')

    parser_pro.add_argument("exp_dir", help="directory containing all the experiment files to be analyzed")
    parser_pro.add_argument("-r", "--reprocess", dest="reprocess", action="store_true",
                            help="reprocess data (look for logs in processed_logs folders)"
                                 "[default: %(default)s]")

    parser_ta.add_argument("exp_dir", help="directory containing all the experiment files to be analyzed")
    parser_ta.add_argument("-r", "--reprocess", dest="reprocess", action="store_true",
                           help="reprocess data (look for logs in provided folders)"
                                "[default: %(default)s]")
    parser_ta.add_argument("-p", "--plot", dest="plot", action="store_true",
                           help="plots the performed analyses"
                                "[default: %(default)s]")
    parser_ta.add_argument("-c", "--collect", dest="collect_all_ta", action="store_true",
                           help="collect some of the main important statistics in one json file "
                                "[default: %(default)s]")
    '''
    parser_ta.add_argument("--executors", dest="executors", type=int,
                           help="executors"
                                "[default: %(default)s]")
    '''
    parser_ta.add_argument("-e", "--extract-essentials", dest="extract_essentials", action="store_true",
                           help='extract essential files to carry on further analysis '
                                '({})'.format(ESSENTIAL_FILES))

    parser_gen.add_argument("exp_dir", help="directory containing all the experiment files to be analyzed")
    parser_gen.add_argument("-i", "--input_num_records", dest="num_records", type=int,
                           help="number of input_records to be considered for the generated json context"
                                "[default: %(default)s]")
    parser_gen.add_argument("-c", "--num-cores", dest="num_cores", type=int,
                           help="number of cores to be considered for the generated json context"
                                "[default: %(default)s]")

    parser_gen.add_argument("-t", "--num-tasks", dest="num_tasks", type=int,
                           help="number of tasks for each stage"
                                "[default: %(default)s]")
    parser_gen.add_argument("-d", "--deadlines", dest="deadlines", type=int, nargs='+',
                           help="deadlines to be considered in json context generation"
                                "[default: %(default)s]")
    parser_gen.add_argument("-v", "--verify", dest="verify", action="store_true",
                            help="launches verification task of the generated file "
                                 "on a remote server ({})".format(D_VERT_SERVER_HOSTNAME))

    parser_pro.set_defaults(func=pro_runner)
    parser_ta.set_defaults(func=time_analysis)
    parser_gen.set_defaults(func=generate_spark_context)
    args = parser.parse_args()

    try:
        getattr(args, "func")
    except AttributeError:
        parser.print_help()
        sys.exit(0)

    args.func(args)
