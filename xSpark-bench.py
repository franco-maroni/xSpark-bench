from config import PROVIDER, NUM_INSTANCE, NUM_RUN, CLUSTER_ID, TERMINATE, RUN, REBOOT, CLUSTER_MAP

import libcloud.common.base
import argparse
import sys

from spark_log_profiling import processing as profiling
from spark_time_analysis import run as run_ta
from colors import header, okblue, okgreen, warning, underline, bold, fail

import util.utils as utils

from factory_methods import BenchInstanceFactory
import argcomplete

libcloud.common.base.RETRY_FAILED_HTTP_REQUESTS = True


def run_xspark(current_cluster, num_instance=NUM_INSTANCE, num_run=NUM_RUN, cluster_id=CLUSTER_ID, terminate=TERMINATE,
               run=RUN, reboot=REBOOT, assume_yes=False):
    """ Main function;
    * Launch spot request of NUMINSTANCE
    * Run Benchmark
    * Download Log
    * Plot data from log
    """
    print(header('run_xspark(num_instance={}, num_run={}, cluster_id={},terminate={}, run={}, reboot={})'
          .format(num_instance, num_run, cluster_id, terminate, run, reboot)))
    # get cfg_file and initialize main settings
    cfg = utils.get_cfg()
    cfg['main'] = {}
    cfg.set('main', 'current_cluster', current_cluster)
    utils.write_cfg(cfg)

    bench_instance = BenchInstanceFactory.get_bench_instance(PROVIDER, cluster_id)
    setup_ok = True

    if num_instance > 0:
        setup_ok = bench_instance.setup(num_instance, assume_yes)

    if reboot:
        bench_instance.reboot()

    if setup_ok and run:
        bench_instance.run(num_run)

    if terminate:
        bench_instance.terminate()



def setup_cluster(cluster, num_instances, assume_yes):
    # termporary structure to save run configuration
    # TODO: IMPROVE THIS
    run_on_setup = {
        'spark': 0,
        'hdfs' : 1
    }
    cluster_id = CLUSTER_MAP[cluster]
    print(bold('Setup {} with {} instances...'.format(cluster_id, num_instances)))
    run_xspark(current_cluster=cluster, num_instance=num_instances, cluster_id=cluster_id,
               run=run_on_setup[cluster], terminate=0, reboot=0, assume_yes=assume_yes)


def kill_cluster(cluster):
    cluster_id = CLUSTER_MAP[cluster]
    print(bold('Terminate {}...'.format(cluster_id)))
    run_xspark(current_cluster=cluster, num_instance=0, cluster_id=cluster_id, run=0, terminate=1, reboot=0)
    cfg = utils.get_cfg()
    cfg[cluster] = {}
    utils.write_cfg(cfg)


def run_log_profiling(local):
    out_dir = None
    in_dir = None
    cfg = utils.get_cfg()
    if not local:
        if 'out_folders' in cfg:
            for x in cfg['out_folders']:
                in_dir = out_dir = cfg['out_folders'][x]
                profiling.main(input_dir=in_dir, json_out_dir=out_dir)
    else:
        profiling.main(input_dir=in_dir, json_out_dir=out_dir)


def run_time_analysis(input_dir):
    if not input_dir:
        cfg = utils.get_cfg()
        if 'out_folders' in cfg:
            for x in cfg['out_folders']:
                input_dir  = cfg['out_folders'][x]
                run_ta.main(input_dir)
    else:
        run_ta.main(input_dir)



def run_check_cluster(cluster):
    cluster_id = CLUSTER_MAP[cluster]
    bench_instance = BenchInstanceFactory.get_bench_instance(PROVIDER, cluster_id)
    print(bench_instance.retrieve_nodes())
    print([(x.name, x.state) for x in bench_instance.nodes])


def setup(args):
    cluster = args.cluster
    num_instances = args.num_instances
    assume_yes = args.assume_yes
    if cluster == 'all':
        setup_cluster('hdfs', num_instances, assume_yes)
        setup_cluster('spark', num_instances, assume_yes)
    else:
        setup_cluster(cluster, num_instances, assume_yes)


def profile(args):
    print(bold('Profile {} performing {} runs...'.format(args.exp_file_path, args.num_runs)))
    raise NotImplementedError()


def submit(args):
    print(bold('Submit {}...'.format(args.exp_file_path)))
    raise NotImplementedError()


def reboot(args):
    cluster = args.cluster
    cluster_id = CLUSTER_MAP[cluster]
    print(bold('Reboot {}...'.format(cluster_id)))
    run_xspark(current_cluster=cluster, num_instance=0, cluster_id=cluster_id, run=0, terminate=0, reboot=1)


def terminate(args):
    cluster = args.cluster
    if cluster == 'all':
        kill_cluster('spark')
        kill_cluster('hdfs')
    else:
        kill_cluster(cluster)


def launch_exp(args):
    cluster_id = CLUSTER_MAP['spark']
    num_v = args.num_v
    num_run = args.num_runs
    for v in num_v:
        cfg = utils.get_cfg()
        cfg['pagerank'] = {}
        cfg['pagerank']['num_v'] = '(2, ' + v + ')'
        utils.write_cfg(cfg)
        print(bold('Launch {} Experiments on {} with {} vertices...'.format(num_run, cluster_id, v)))
        run_xspark(current_cluster='spark', num_instance=0, num_run=num_run,
                   cluster_id=cluster_id, run=1, terminate=0, reboot=0)
        if args.profile:
            run_log_profiling(None)
        if args.time_analysis:
            run_time_analysis(None)


def log_profiling(args):
    run_log_profiling(args.local)


def time_analysis(args):
    run_time_analysis(args.input_dir)

def check_clusters(args):
    cluster = args.cluster
    if cluster == 'all':
        run_check_cluster('spark')
        run_check_cluster('hdfs')
    else:
        run_check_cluster(cluster)


def main():
    parser = argparse.ArgumentParser(
        description=
            """
            xSpark Client
            """
    )

    subparsers = parser.add_subparsers()

    parser_setup = subparsers.add_parser('setup', help='add n nodes to the specified cluster')
    parser_launch_exp = subparsers.add_parser('launch_exp', help='launch experiments on already deployed spark cluster')
    parser_reboot = subparsers.add_parser('reboot', help='reboots all the nodes of the specified cluster')
    parser_terminate = subparsers.add_parser('terminate', help='terminates all the nodes in the specified cluster')
    parser_log_profiling = subparsers.add_parser('log_profiling', help='runs the log_profiling')
    parser_time_analysis = subparsers.add_parser('time_analysis', help='runs the time_analysis')
    parser_check_cluster = subparsers.add_parser('check', help='checks the status of the specified cluster')
    '''
    parser_profile = subparsers.add_parser('profile', help='profiles and averages r times the specified application, '
                                                           'deploys the profiling file in xSpark and downloads the '
                                                          'results into the client machine')
    parser_submit = subparsers.add_parser('submit', help='submits the specified application and downloads the results '
                                                         'into the client machine')
    '''

    parser_setup.add_argument('cluster', choices=['hdfs', 'spark', 'all'], help='The specified cluster')
    parser_setup.add_argument('-n', '--num-instances', type=int, default=5, dest='num_instances',
                              help='Number of instances to be created per cluster')
    parser_setup.add_argument('-y', '--yes', dest='assume_yes', action='store_true',
                              help='Assume yes to the confirmation queries')

    parser_reboot.add_argument('cluster', choices=['hdfs', 'spark', 'all'], help='The specified cluster')

    parser_terminate.add_argument('cluster', choices=['hdfs', 'spark', 'all'], help='The specified cluster')

    parser_launch_exp.add_argument('-v', '--num-v', dest='num_v', nargs='+', required=True, help="number of vertices")
    parser_launch_exp.add_argument('-r', '--num-runs', default=1, type=int, dest='num_runs',
                                   help='Number of runs for each configuration')
    parser_launch_exp.add_argument("-p", "--profile", dest="profile", action="store_true",
                                   help="perform log profiling at the end of experiments"
                                        "[default: %(default)s]")
    parser_launch_exp.add_argument("-t", "--time_analysis", dest="time_analysis", action="store_true",
                                   help="perform time analysis at the end of experiments"
                                        "[default: %(default)s]")

    parser_log_profiling.add_argument("-l", "--local", dest="local", action="store_true",
                                      help="use default local output folders"
                                           "[default: %(default)s]")

    parser_time_analysis.add_argument("-i", "--input-dir", dest="input_dir",
                                      help="input directory (where all the log files are located)"
                                           "[default: load from config file latest benchmark directory")

    parser_check_cluster.add_argument('cluster', choices=['hdfs', 'spark', 'all'], help='The specified cluster')

    '''
    parser_profile.add_argument('exp_file_path', help='experiment file path')
    parser_profile.add_argument('-r', '--num-runs', default=1, type=int, dest='num_runs', help='Number of runs')

    parser_submit.add_argument('exp_file_path', help='experiment file path')
    '''

    parser_setup.set_defaults(func=setup)
    parser_reboot.set_defaults(func=reboot)
    parser_terminate.set_defaults(func=terminate)
    '''
    parser_profile.set_defaults(func=profile)
    parser_submit.set_defaults(func=submit)
    '''

    parser_launch_exp.set_defaults(func=launch_exp)
    parser_log_profiling.set_defaults(func=log_profiling)
    parser_time_analysis.set_defaults(func=time_analysis)
    parser_check_cluster.set_defaults(func=check_clusters)

    argcomplete.autocomplete(parser)
    args = parser.parse_args()


    try:
        getattr(args, "func")
    except AttributeError:
        parser.print_help()
        sys.exit(0)

    args.func(args)


if __name__ == "__main__":
    main()
