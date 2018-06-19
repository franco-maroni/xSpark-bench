"""
This module handle the configuration of the instances and the execution of  the benchmark on the cluster
"""

import json
import multiprocessing
import os
import time
from ast import literal_eval
from concurrent.futures import ThreadPoolExecutor

import log
from config import PRIVATE_KEY_PATH, PRIVATE_KEY_NAME, TEMPORARY_STORAGE, PROVIDER
from config import UPDATE_SPARK_DOCKER, DELETE_HDFS, SPARK_HOME, KILL_JAVA, SYNC_TIME, \
    UPDATE_SPARK, \
    DISABLE_HT, ENABLE_EXTERNAL_SHUFFLE, OFF_HEAP, OFF_HEAP_BYTES, K, T_SAMPLE, TI, CORE_QUANTUM, \
    CORE_MIN, CPU_PERIOD, \
    CORE_VM, UPDATE_SPARK_MASTER, DEADLINE, MAX_EXECUTOR, ALPHA, BETA, OVER_SCALE, LOCALITY_WAIT, \
    LOCALITY_WAIT_NODE, CPU_TASK, \
    LOCALITY_WAIT_PROCESS, LOCALITY_WAIT_RACK, INPUT_RECORD, NUM_TASK, BENCH_NUM_TRIALS, \
    SCALE_FACTOR, RAM_EXEC, \
    RAM_DRIVER, BENCHMARK_PERF, BENCH_LINES, HADOOP_CONF, \
    CONFIG_DICT, HADOOP_HOME,\
    SPARK_2_HOME, BENCHMARK_BENCH, BENCH_CONF, LOG_LEVEL, CORE_ALLOCATION,DEADLINE_ALLOCATION,\
    UPDATE_SPARK_BENCH, UPDATE_SPARK_PERF, SPARK_PERF_FOLDER, NUM_INSTANCE, STAGE_ALLOCATION, HEURISTIC, VAR_PAR_MAP
from util.ssh_client import sshclient_from_node, sshclient_from_ip
from util.utils import timing, between, get_cfg, write_cfg, open_cfg


# Modifiche fatte
# - uso di PRIVATE_KEY_PATH anzichè KEY_PAIR_PATH
# - uso di PRIVATE_KEY_NAME anzichè DATA_AMI[REGION]["keypair"]+".pem"
# - uso di TEMPORARY_STORAGE anzichè "/dev/xvdb"
# - uso di ssh_sshclient_from_node per ottenere un client ssh basato su ParamikoSSHClient
# - uso di ip privati in azure, altrimenti fallisce il binding sulle porte di hadoop


def get_ip(node):
    if PROVIDER == "AWS_SPOT":
        return node.extra['dns_name']
    if PROVIDER == "AZURE":
        return node.private_ips[0]

def common_setup(ssh_client):
    """
    Common setup of the instance of the cluster with ssh_client is connected

    :param ssh_client: the ssh client to launch command on the instance
    :return: nothing
    """
    with open_cfg() as cfg:
        delete_hdfs = cfg.getboolean('main', 'delete_hdfs')
    #  preliminary steps required due to differences between azure and aws
    if PROVIDER == "AZURE":

        # todo only if first run
        if NUM_INSTANCE > 0:
            # add ssh key that matches the public one used during creation
            if not "id_rsa" in ssh_client.listdir("/home/ubuntu/.ssh/"):
                ssh_client.put(localpath=PRIVATE_KEY_PATH, remotepath="/home/ubuntu/.ssh/id_rsa")
            ssh_client.run("chmod 400 /home/ubuntu/.ssh/id_rsa")

            # ssh_client.run("sudo groupadd supergroup")
            ssh_client.run("sudo usermod -aG supergroup $USER")
            ssh_client.run("sudo usermod -aG supergroup root")

            # join docker group
            ssh_client.run("sudo usermod -aG docker $USER")

            # ssh_client.run("mkdir /usr/local/spark/spark-events")

            # ssh_client.run("sudo chmod -R 777 /mnt")

            # to refresh groups
            ssh_client.close()
            ssh_client.connect()

            # restore environmental variables lost when creating the image
            ssh_client.run("echo 'export JAVA_HOME=/usr/lib/jvm/java-8-oracle' >> $HOME/.bashrc")
            ssh_client.run("echo 'export HADOOP_INSTALL=/usr/local/lib/hadoop-2.7.2' >> $HOME/.bashrc")
            ssh_client.run("echo 'export PATH=$PATH:$HADOOP_INSTALL/bin' >> $HOME/.bashrc")
            ssh_client.run("echo 'export PATH=$PATH:$HADOOP_INSTALL/sbin' >> $HOME/.bashrc")
            ssh_client.run("echo 'export HADOOP_MAPRED_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
            ssh_client.run("echo 'export HADOOP_COMMON_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
            ssh_client.run("echo 'export HADOOP_HDFS_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
            ssh_client.run("echo 'export HADOOP_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
            ssh_client.run("echo 'export YARN_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
            ssh_client.run("echo 'export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_INSTALL/lib/native/' >> $HOME/.bashrc")
            ssh_client.run("echo 'export HADOOP_OPTS=\"-Djava.library.path=$HADOOP_INSTALL/lib/native\"'  >> $HOME/.bashrc")
            ssh_client.run(
                "echo 'export LD_LIBRARY_PATH=$HADOOP_INSTALL/lib/native:$LD_LIBRARY_PATH' >> $HOME/.bashrc")  # to fix "unable to load native hadoop lib" in spark

            ssh_client.run("source $HOME/.bashrc")


        # # PageRank
        # ssh_client.run("rm /usr/local/spark/conf/Spark_PageRank_Application.json")
        # ssh_client.put(localpath="C:\\workspace\\spark-log-profiling\\output_json\\Spark_PageRank_Application_20170523133037.json",
        #                remotepath="/usr/local/spark/conf/Spark_PageRank_Application.json")
        #
        #
        # # DecisionTree
        # ssh_client.run("rm /usr/local/spark/conf/DecisionTree_classification_Example.json")
        # ssh_client.put(
        #     localpath="C:\\workspace\\spark-log-profiling\\output_json\\DecisionTree_classification_Example_20170523134646.json",
        #     remotepath="/usr/local/spark/conf/DecisionTree_classification_Example.json")
        #
        # # Kmeans
        # ssh_client.run("rm /usr/local/spark/conf/Spark_KMeans_Example.json")
        # ssh_client.put(localpath="C:\\workspace\\spark-log-profiling\\output_json\\Spark_KMeans_Example_20170509081738.json",
        #                remotepath="/usr/local/spark/conf/Spark_KMeans_Example.json")
        #
        # # SVM
        # ssh_client.run("rm /usr/local/spark/conf/SVM_Classifier_Example.json")
        # ssh_client.put(localpath="C:\\workspace\\spark-log-profiling\\output_json\\SVM_Classifier_Example_20170509105715.json",
        #                remotepath="/usr/local/spark/conf/SVM_Classifier_Example.json")
        #
        # # Agg by key
        # ssh_client.run("rm /usr/local/spark/conf/TestRunner__aggregate-by-key.json")
        # ssh_client.put(
        # localpath="C:\\workspace\\spark-log-profiling\\output_json\\TestRunner__aggregate-by-key_20170511110351.json",
        #                remotepath="/usr/local/spark/conf/TestRunner__aggregate-by-key.json")
        #
        # # Agg by key int
        # ssh_client.run("rm /usr/local/spark/conf/TestRunner__aggregate-by-key-int.json")
        # ssh_client.put(
        # localpath="C:\\workspace\\spark-log-profiling\\output_json\\TestRunner__aggregate-by-key-int_20170511112110.json",
        #                remotepath="/usr/local/spark/conf/TestRunner__aggregate-by-key-int.json")
        #
        # # Agg by key naive
        # ssh_client.run("rm /usr/local/spark/conf/TestRunner__aggregate-by-key-naive.json")
        # ssh_client.put(
        # localpath="C:\\workspace\\spark-log-profiling\\output_json\\TestRunner__aggregate-by-key-naive_20170511114259.json",
        #                remotepath="/usr/local/spark/conf/TestRunner__aggregate-by-key-naive.json")
        #
        # # Sort by key
        # ssh_client.run("rm /usr/local/spark/conf/TestRunner__sort-by-key.json")
        # ssh_client.put(
        # localpath="C:\\workspace\\spark-log-profiling\\output_json\\TestRunner__sort-by-key_20170511131321.json",
        #                remotepath="/usr/local/spark/conf/TestRunner__sort-by-key.json")
        #
        # # Sort by key int
        # ssh_client.run("rm /usr/local/spark/conf/TestRunner__sort-by-key-int.json")
        # ssh_client.put(
        # localpath="C:\\workspace\\spark-log-profiling\\output_json\\TestRunner__sort-by-key-int_20170511133334.json",
        #                remotepath="/usr/local/spark/conf/TestRunner__sort-by-key-int.json")
        #
        # # Count
        # ssh_client.run("rm /usr/local/spark/conf/TestRunner__count.json")
        # ssh_client.put(
        # localpath="C:\\workspace\\spark-log-profiling\\output_json\\TestRunner__count_20170511135036.json",
        #                remotepath="/usr/local/spark/conf/TestRunner__count.json")
        #
        # # Count with filtr
        # ssh_client.run("rm /usr/local/spark/conf/TestRunner__count-with-filter.json")
        # ssh_client.put(
        # localpath="C:\\workspace\\spark-log-profiling\\output_json\\TestRunner__count-with-filter_20170511140627.json",
        #                remotepath="/usr/local/spark/conf/TestRunner__count-with-filter.json")


    if PROVIDER == "AWS_SPOT":
        ssh_client.run("echo 'export JAVA_HOME=/usr/lib/jvm/java-8-oracle' >> $HOME/.bashrc")
        ssh_client.run("echo 'export HADOOP_INSTALL=/usr/local/lib/hadoop-2.7.2' >> $HOME/.bashrc")
        ssh_client.run("echo 'export PATH=$PATH:$HADOOP_INSTALL/bin' >> $HOME/.bashrc")
        ssh_client.run("echo 'export PATH=$PATH:$HADOOP_INSTALL/sbin' >> $HOME/.bashrc")
        ssh_client.run("echo 'export HADOOP_MAPRED_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
        ssh_client.run("echo 'export HADOOP_COMMON_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
        ssh_client.run("echo 'export HADOOP_HDFS_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
        ssh_client.run("echo 'export HADOOP_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
        ssh_client.run("echo 'export YARN_HOME=$HADOOP_INSTALL' >> $HOME/.bashrc")
        ssh_client.run("echo 'export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_INSTALL/lib/native' >> $HOME/.bashrc")
        ssh_client.run("echo 'export HADOOP_OPTS=\"-Djava.library.path=$HADOOP_INSTALL/lib/native\"'  >> $HOME/.bashrc")
        ssh_client.run(
            "echo 'export LD_LIBRARY_PATH=$HADOOP_INSTALL/lib/native:$LD_LIBRARY_PATH' >> $HOME/.bashrc")  # to fix "unable to load native hadoop lib" in spark
        ssh_client.run("source $HOME/.bashrc")


    ssh_client.run("export GOMAXPROCS=`nproc`")

    if UPDATE_SPARK_DOCKER:
        print("   Updating Spark Docker Image...")
        ssh_client.run("docker pull elfolink/spark:2.0")

    if delete_hdfs:
        ssh_client.run("sudo umount /mnt")
        ssh_client.run(
                "sudo mkfs.ext4 -E nodiscard " + TEMPORARY_STORAGE + " && sudo mount -o discard " + TEMPORARY_STORAGE + " /mnt")

    ssh_client.run("test -d /mnt/tmp || sudo mkdir -m 1777 /mnt/tmp")
    ssh_client.run("sudo mount --bind /mnt/tmp /tmp")

    ssh_client.run('ssh-keygen -f "/home/ubuntu/.ssh/known_hosts" -R localhost')

    print("   Stop Spark Slave/Master")
    # ssh_client.run('export SPARK_HOME="{s}" && {s}sbin/stop-slave.sh'.format(s=SPARK_HOME))
    ssh_client.run('export SPARK_HOME="{s}" && {s}sbin/stop-master.sh'.format(s=SPARK_HOME))
    ssh_client.run('export SPARK_HOME="{s}" && sudo {s}sbin/stop-slave.sh'.format(s=SPARK_HOME))
    print("   Set Log Level")
    ssh_client.run(
        "sed -i '19s/.*/log4j.rootCategory={}, console /' {}conf/log4j.properties".format(LOG_LEVEL,
                                                                                          SPARK_HOME))

    if KILL_JAVA:
        print("   Killing Java")
        ssh_client.run('sudo killall java && sudo killall java && sudo killall java')

    print("   Kill SAR CPU Logger")
    ssh_client.run("screen -ls | grep Detached | cut -d. -f1 | awk '{print $1}' | xargs -r kill")

    if SYNC_TIME:
        print("   SYNC TIME")
        ssh_client.run("sudo ntpdate -s time.nist.gov")

    print("   Removing Stopped Docker")
    ssh_client.run("docker ps -a | awk '{print $1}' | xargs --no-run-if-empty docker rm")


@timing
def setup_slave(node, master_ip, count):
    """

    :param node:
    :param master_ip:
    :return:
    """
    cfg = get_cfg()
    current_cluster = cfg['main']['current_cluster']

    ssh_client = sshclient_from_node(node, ssh_key_file=PRIVATE_KEY_PATH, user_name='ubuntu')

    print("Setup Slave: PublicIp=" + node.public_ips[0] + " PrivateIp=" + node.private_ips[0])

    slave_ip = get_ip(node)

    # cfg[current_cluster]['slave'+count+'_ip'] = slave_ip
    # write_cfg(cfg)

    common_setup(ssh_client)

    if UPDATE_SPARK:
        print("   Updating Spark...")
        ssh_client.run(
            """cd /usr/local/spark && git pull && build/mvn clean && build/mvn -T 1C -Phive -Pnetlib-lgpl -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.2 -Dscala-2.11 -DskipTests -Dmaven.test.skip=true package""")

    # CLEAN UP EXECUTORS APP LOGS
    # ssh_client.run("rm -r " + SPARK_HOME + "work/*")
    ssh_client.run("sudo rm -r " + SPARK_HOME + "work/*")

    if DISABLE_HT:
        # DISABLE HT
        ssh_client.put(localpath="./disable-ht-v2.sh", remotepath="$HOME/disable-ht-v2.sh")
        ssh_client.run("chmod +x $HOME/disable-ht-v2.sh")
        stdout, stderr, status = ssh_client.run('sudo $HOME/disable-ht-v2.sh')
        print("   Disabled HyperThreading {}".format(status))

    if current_cluster == 'spark':
    # Modificato questo
        ssh_client.run(
            "sed -i '31s/.*/spark.shuffle.service.enabled {0}/' {1}conf/spark-defaults.conf".format(
                ENABLE_EXTERNAL_SHUFFLE, SPARK_HOME))

        # ssh_client.run('echo "spark.local.dir /mnt/hdfs" >> '+ SPARK_HOME + 'conf/spark-defaults.conf')

        ssh_client.run(
            "sed -i '32s/.*/spark.memory.offHeap.enabled {0}/' {1}conf/spark-defaults.conf".format(
                OFF_HEAP, SPARK_HOME))
        ssh_client.run(
            "sed -i '33s/.*/spark.memory.offHeap.size {0}/' {1}conf/spark-defaults.conf".format(
                OFF_HEAP_BYTES, SPARK_HOME))

        ssh_client.run("sed -i '42s/.*/spark.control.k {0}/' {1}conf/spark-defaults.conf".format(
            K, SPARK_HOME))

        # SAMPLING RATE LINE 43
        ssh_client.run("sed -i '43s/.*/spark.control.tsample {0}/' {1}conf/spark-defaults.conf".format(
            T_SAMPLE, SPARK_HOME))

        ssh_client.run("sed -i '44s/.*/spark.control.ti {0}/' {1}conf/spark-defaults.conf".format(
            TI, SPARK_HOME))

        ssh_client.run("sed -i '45s{.*{spark.control.corequantum " + str(
            CORE_QUANTUM) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run("sed -i '50s{.*{spark.control.coremin " + str(
            CORE_MIN) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run("sed -i '41s{.*{spark.control.cpuperiod " + str(
            CPU_PERIOD) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

    if current_cluster == 'spark':
        print("   Starting Spark Slave")
        ssh_client.run(
            # 'export SPARK_HOME="{s}" && {s}sbin/start-slave.sh {0}:7077 -h {1}  --port 9999 -c {2}'.format(
            'export SPARK_HOME="{s}" && sudo {s}sbin/start-slave.sh {0}:7077 -h {1}  --port 9999 -c {2}'.format(

            master_ip, slave_ip, CORE_VM, s=SPARK_HOME))
        # REAL CPU LOG
        log_cpu_command = 'screen -d -m -S "{0}" bash -c "sar -u 1 > sar-{1}.log"'.format(
            slave_ip, slave_ip)
        print("   Start SAR CPU Logging")
        ssh_client.run(log_cpu_command)


@timing
def setup_master(node, slaves_ip, hdfs_master):
    """
    :param node:
    :return:
    """
    ssh_client = sshclient_from_node(node, ssh_key_file=PRIVATE_KEY_PATH, user_name='ubuntu')
    with open_cfg(mode='w') as cfg:
        current_cluster = cfg['main']['current_cluster']
        benchmark = cfg['main']['benchmark'] if 'main' in cfg and 'benchmark' in cfg['main'] else ''
        cfg[current_cluster] = {}
        # TODO check if needed
        input_record = cfg['pagerank']['num_v'] if 'pagerank' in cfg and 'num_v' in cfg['pagerank'] else INPUT_RECORD
        print("input_record: {}".format(input_record))

        print("Setup Master: PublicIp=" + node.public_ips[0] + " PrivateIp=" + node.private_ips[0])
        master_private_ip = get_ip(node)
        master_public_ip = node.public_ips[0]

        # save private master_ip to cfg file
        print('saving master ip')
        cfg[current_cluster]['master_private_ip'] = master_private_ip
        cfg[current_cluster]['master_public_ip'] = master_public_ip

    common_setup(ssh_client)



    # update spark-bench and spark-perf

    ssh_client.run("sudo mv /usr/local/spark-perf/ /home/ubuntu/")
    ssh_client.run("sudo mv /usr/local/spark-bench/ /home/ubuntu/")
    ssh_client.run("sudo mv /usr/local/wikixmlj/ /home/ubuntu/")


    files = ssh_client.listdir("/home/ubuntu/")
    # download or update
    if UPDATE_SPARK_PERF:
        if "wikixmlj" in files:
            ssh_client.run("""cd $HOME/wikixmlj && git status | grep "up-to-date" || eval `git pull && mvn package install`""")
            ssh_client.run("cd $HOME")
        else:
            ssh_client.run("git clone https://github.com/synhershko/wikixmlj.git wikixmlj")
            ssh_client.run(
                "cd $HOME/wikixmlj && mvn package install -Dmaven.test.skip=true && cd $HOME")  # install wikixmlj

        if "spark-perf" in files:
            ssh_client.run("""cd $HOME/spark-perf && git status | grep "up-to-date" || eval `git pull && cp $HOME/spark-perf/config/config.py.template $HOME/spark-perf/config/config.py`""")
            ssh_client.run("cd $HOME")
        else:
            ssh_client.run("git clone https://github.com/databricks/spark-perf.git spark-perf")
            ssh_client.run(
                "cp $HOME/spark-perf/config/config.py.template $HOME/spark-perf/config/config.py")

    if UPDATE_SPARK_BENCH:
        if "spark-bench" in files:
            ssh_client.run("""cd $HOME/spark-bench && git status | grep "up-to-date" || eval `git pull && sed -i '7s{.*{mvn package -P spark2.0{' $HOME/spark-bench/bin/build-all.sh && $HOME/spark-bench/bin/build-all.sh && cp $HOME/spark-bench/conf/env.sh.template $HOME/spark-bench/conf/env.sh`""")
            ssh_client.run("cd $HOME")
        else:
            ssh_client.run("git clone https://github.com/gioenn/spark-bench.git spark-bench")
            ssh_client.run("$HOME/spark-bench/bin/build-all.sh")  # build spark-bench
            ssh_client.run("cp $HOME/spark-bench/conf/env.sh.template $HOME/spark-bench/conf/env.sh")  # copy spark-bench config



    # ssh_client.run("cd $HOME/wikixmlj && mvn package install -Dmaven.test.skip=true && cd $HOME")  # install wikixmlj
    # ssh_client.run("sed -i '7s{.*{mvn package -P spark2.0{' $HOME/spark-bench/bin/build-all.sh")
    # ssh_client.run("$HOME/spark-bench/bin/build-all.sh") # build spark-bench
    # ssh_client.run("cp $HOME/spark-bench/conf/env.sh.template $HOME/spark-bench/conf/env.sh")  # copy spark-bench config
    # ssh_client.run(
    #     "cp $HOME/spark-perf/config/config.py.template $HOME/spark-perf/config/config.py")  # copy spark-perf config

    # copia chiave privata
    if not PRIVATE_KEY_NAME in files:
        ssh_client.put(localpath=PRIVATE_KEY_PATH, remotepath="/home/ubuntu/" + PRIVATE_KEY_NAME)
        ssh_client.run("chmod 400 " + "$HOME/" + PRIVATE_KEY_NAME)

    if UPDATE_SPARK_MASTER:
        print("   Updating Spark...")
        ssh_client.run(
            """cd /usr/local/spark && git pull && build/mvn clean &&  build/mvn -T 1C -Phive -Pnetlib-lgpl -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.2 -Dscala-2.11 -DskipTests -Dmaven.test.skip=true package""")

    print("   Remove Logs")
    # ssh_client.run("rm " + SPARK_HOME + "spark-events/*")
    ssh_client.run("sudo rm " + SPARK_HOME + "spark-events/*")

    # TODO Check number of lines in spark-defaults.conf

    # SHUFFLE SERVICE EXTERNAL
    ssh_client.run(
        "sed -i '31s/.*/spark.shuffle.service.enabled {0}/' {1}conf/spark-defaults.conf".format(
            ENABLE_EXTERNAL_SHUFFLE, SPARK_HOME))

    if current_cluster == 'spark':

        # TODO: check this!!
        scale_factor = SCALE_FACTOR
        num_task = NUM_TASK

        if benchmark == 'sort_by_key':
            BENCH_CONF['scala-sort-by-key']['ScaleFactor'] = literal_eval(cfg['sort_by_key']['scale_factor'])[1]
            print('setting ScaleFactor as {}'.format(BENCH_CONF['scala-sort-by-key']['ScaleFactor']))
            BENCH_CONF['scala-sort-by-key']['num-partitions'] = cfg['sort_by_key']['num_partitions']
            print('setting num-partitions as {}'.format(BENCH_CONF['scala-sort-by-key']['num-partitions']))
            scale_factor = BENCH_CONF[BENCHMARK_PERF[0]]["ScaleFactor"]
            input_record = 200 * 1000 * 1000 * scale_factor
            num_task = scale_factor

        # OFF HEAP
        ssh_client.run(
            "sed -i '32s{.*{spark.memory.offHeap.enabled " + str(
                OFF_HEAP) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")
        ssh_client.run(
            "sed -i '33s{.*{spark.memory.offHeap.size " + str(
                OFF_HEAP_BYTES) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        print("   Changing Benchmark settings")
        # DEADLINE LINE 35
        ssh_client.run("sed -i '35s{.*{spark.control.deadline " + str(
            DEADLINE) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        # MAX EXECUTOR LINE 39
        ssh_client.run("sed -i '39s{.*{spark.control.maxexecutor " + str(
            MAX_EXECUTOR) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        # CORE FOR VM LINE 40
        ssh_client.run("sed -i '40s{.*{spark.control.coreforvm " + str(
            CORE_VM) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        # ALPHA LINE 36
        ssh_client.run("sed -i '36s{.*{spark.control.alpha " + str(
            ALPHA) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        # BETA line 37
        ssh_client.run("sed -i '37s{.*{spark.control.beta " + str(
            BETA) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        # OVERSCALE LINE 38
        ssh_client.run("sed -i '38s{.*{spark.control.overscale " + str(
            OVER_SCALE) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        # HEURISTIC TYPE LINE 56
        ssh_client.run("sed -i '56s{.*{spark.control.heuristic " + str(
            HEURISTIC.value) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")
        # CORE_ALLOCATION
        if CORE_ALLOCATION != None and DEADLINE_ALLOCATION != None and STAGE_ALLOCATION != None:
            ssh_client.run("sed -i '57s{.*{spark.control.stage " + str(STAGE_ALLOCATION) + "{' "+SPARK_HOME+"conf/spark-defaults.conf")
            ssh_client.run("sed -i '58s{.*{spark.control.stagecores "+ str(CORE_ALLOCATION) +"{' "+SPARK_HOME + "conf/spark-defaults.conf")
            ssh_client.run("sed -i '59s{.*{spark.control.stagedeadlines " + str(
                DEADLINE_ALLOCATION) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")
        else:
            ssh_client.run("sed -i '57s{.*{#stage{' " + SPARK_HOME + "conf/spark-defaults.conf")
            ssh_client.run("sed -i '58s{.*{#stagecores{' "+SPARK_HOME + "conf/spark-defaults.conf")
            ssh_client.run("sed -i '59s{.*{#stagedeadlines{' " + SPARK_HOME + "conf/spark-defaults.conf")

        # CHANGE ALSO IN MASTER FOR THE LOGS
        ssh_client.run(
            "sed -i '43s{.*{spark.control.tsample " + str(
                T_SAMPLE) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run("sed -i '42s{.*{spark.control.k " + str(
            K) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run("sed -i '44s{.*{spark.control.ti " + str(
            TI) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run("sed -i '45s{.*{spark.control.corequantum " + str(
            CORE_QUANTUM) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run(
            "sed -i '46s{.*{spark.locality.wait " + str(
                LOCALITY_WAIT) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run(
            "sed -i '51s{.*{spark.locality.wait.node " + str(
                LOCALITY_WAIT_NODE) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run(
            "sed -i '52s{.*{spark.locality.wait.process " + str(
                LOCALITY_WAIT_PROCESS) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run(
            "sed -i '53s{.*{spark.locality.wait.rack " + str(
                LOCALITY_WAIT_RACK) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run(
            "sed -i '47s{.*{spark.task.cpus " + str(
                CPU_TASK) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run(
            "sed -i '48s{.*{spark.control.nominalrate 0.0{' " + SPARK_HOME + "conf/spark-defaults.conf")
        ssh_client.run(
            "sed -i '49s{.*{spark.control.nominalratedata 0.0{' " + SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run("sed -i '50s{.*{spark.control.coremin " + str(
            CORE_MIN) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run(
            "sed -i '54s{.*{spark.control.inputrecord " + str(
                input_record) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")
        ssh_client.run(
            "sed -i '55s{.*{spark.control.numtask " + str(
                num_task) + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run("""sed -i '3s{.*{master=""" + master_private_ip +
                       """{' ./spark-bench/conf/env.sh""")
        ssh_client.run("""sed -i '63s{.*{NUM_TRIALS=""" + str(BENCH_NUM_TRIALS) +
                       """{' ./spark-bench/conf/env.sh""")

        # CHANGE SPARK HOME DIR
        ssh_client.run("sed -i '21s{.*{SPARK_HOME_DIR = \"" + SPARK_HOME + "\"{' ./spark-perf/config/config.py")

        # CHANGE MASTER ADDRESS IN BENCHMARK
        ssh_client.run("""sed -i '30s{.*{SPARK_CLUSTER_URL = "spark://""" + master_private_ip +
                       """:7077"{' ./spark-perf/config/config.py""")

        # CHANGE SCALE FACTOR LINE 127
        ssh_client.run(
            "sed -i '127s{.*{SCALE_FACTOR = " + str(scale_factor) + "{' ./spark-perf/config/config.py")

        # NO PROMPT
        ssh_client.run("sed -i '103s{.*{PROMPT_FOR_DELETES = False{' ./spark-perf/config/config.py")
        if len(BENCHMARK_PERF) > 0:  # and SPARK_PERF_FOLDER == "spark-perf-gioenn":
            # print("   Setting up skewed test")
            # ssh_client.run("""sed -i '164s{.*{OptionSet("skew", [""" + str(
            #     BENCH_CONF[BENCHMARK_PERF[0]]["skew"]) + """]){' ./""" + SPARK_PERF_FOLDER + "/config/config.py")
            print("   Setting up unique-keys, num-partitions and reduce-tasks")
            ssh_client.run("""sed -i '185s{.*{OptionSet("unique-keys",[""" + str(BENCH_CONF[BENCHMARK_PERF[0]][
                                                                                     "unique-keys"]) + """], False),{' ./""" + SPARK_PERF_FOLDER + "/config/config.py")
            ssh_client.run("""sed -i '170s{.*{OptionSet("num-partitions", [""" + str(BENCH_CONF[BENCHMARK_PERF[0]][
                                                                                         "num-partitions"]) + """], can_scale=False),{' ./""" + SPARK_PERF_FOLDER + "/config/config.py")
            ssh_client.run("""sed -i '172s{.*{OptionSet("reduce-tasks", [""" + str(BENCH_CONF[BENCHMARK_PERF[0]][
                                                                                       "reduce-tasks"]) + """], can_scale=False),{' ./""" + SPARK_PERF_FOLDER + "/config/config.py")

        # CHANGE RAM EXEC
        ssh_client.run(
            """sed -i '146s{.*{    JavaOptionSet("spark.executor.memory", [""" + RAM_EXEC + """]),{' ./spark-perf/config/config.py""")

        ssh_client.run(
            """sed -i '55s{.*{SPARK_EXECUTOR_MEMORY=""" + RAM_EXEC + """{' ./spark-bench/conf/env.sh""")

        # CHANGE RAM DRIVER
        ssh_client.run(
            "sed -i '26s{.*{spark.driver.memory " + RAM_DRIVER + "{' " + SPARK_HOME + "conf/spark-defaults.conf")

        ssh_client.run(
            "sed -i '154s{.*{SPARK_DRIVER_MEMORY = \""+RAM_DRIVER+"\"{' ./spark-perf/config/config.py")


        print("   Enabling/Disabling Benchmark")
        # ENABLE BENCHMARK
        for bench in BENCHMARK_PERF:
            for line_number in BENCH_LINES[bench]:
                sed_command_line = "sed -i '" + line_number + " s/[#]//g' ./spark-perf/config/config.py"
                ssh_client.run(sed_command_line)

        # DISABLE BENCHMARK
        for bench in BENCH_LINES:
            if bench not in BENCHMARK_PERF:
                for line_number in BENCH_LINES[bench]:
                    ssh_client.run("sed -i '" + line_number + " s/^/#/' ./spark-perf/config/config.py")

        # ENABLE HDFS
        # if HDFS:
        print("   Enabling HDFS in benchmarks")
        ssh_client.run("sed -i '179s%memory%hdfs%g' ./spark-perf/config/config.py")

    #if hdfs_master != "":
        ssh_client.run(
            """sed -i  '50s%.*%HDFS_URL = "hdfs://{0}:9000/test/"%' ./spark-perf/config/config.py""".format(
                hdfs_master))
        ssh_client.run(
            """sed -i  '10s%.*%HDFS_URL="hdfs://{0}:9000"%' ./spark-bench/conf/env.sh""".format(
                hdfs_master))
        ssh_client.run(
            """sed -i  '14s%.*%DATA_HDFS="hdfs://{0}:9000/SparkBench"%' ./spark-bench/conf/env.sh""".format(
                hdfs_master))

        # TODO: additional settings for spark-bench
        # ssh_client.run("""sed -i '8s/.*//' ./spark-bench/conf/env.sh""")
        ssh_client.run("""sed -i '8s{.*{[ -z "$HADOOP_HOME" ] \&\&     export HADOOP_HOME="""+HADOOP_HOME+"""{' ./spark-bench/conf/env.sh""")
        # slaves = slaves_ip[0]
        # for slave in slaves_ip[1:]:
        #     slaves = slaves + ", " + slave

        ssh_client.run(
            """sed -i  '5s%.*%MC_LIST=()%' ./spark-bench/conf/env.sh""")

        ssh_client.run(
            """sed -i  '19s%.*%SPARK_VERSION=2.0%' ./spark-bench/conf/env.sh""")

        # ssh_client.run("""sed -i '20s/.*//' ./spark-bench/conf/env.sh""")
        ssh_client.run("""sed -i '20s{.*{[ -z "$SPARK_HOME" ] \&\&     export SPARK_HOME="""+SPARK_HOME+"""{' ./spark-bench/conf/env.sh""")

    # START MASTER and HISTORY SERVER
    if current_cluster == 'spark':
        print("   Starting Spark Master")
        ssh_client.run(
            'export SPARK_HOME="{d}" && {d}sbin/start-master.sh -h {0}'.format(
                master_private_ip, d=SPARK_HOME))
        print("   Starting Spark History Server")
        ssh_client.run(
            'export SPARK_HOME="{d}" && {d}sbin/start-history-server.sh'.format(d=SPARK_HOME))

    return master_private_ip, node


@timing
def setup_hdfs_ssd(node, hdfs_master):
    """

    :param node:
    :return:
    """
    with open_cfg() as cfg:
        delete_hdfs = cfg.getboolean('main', 'delete_hdfs')
    ssh_client = sshclient_from_node(node, ssh_key_file=PRIVATE_KEY_PATH, user_name='ubuntu')

    out, err, status = ssh_client.run(
        """test -d /mnt/hdfs/namenode || sudo mkdir --parents /mnt/hdfs/namenode &&
        sudo mkdir --parents /mnt/hdfs/datanode""")
    if status != 0:
        print(out, err)
    # if PROVIDER == "AWS_SPOT":
    #     ssh_client.run("sudo chown ubuntu:hadoop /mnt/hdfs && sudo chown ubuntu:hadoop /mnt/hdfs/*")
    # elif PROVIDER == "AZURE":
    ssh_client.run("sudo chown ubuntu:ubuntu /mnt/hdfs && sudo chown ubuntu:ubuntu /mnt/hdfs/*")
    if delete_hdfs or hdfs_master == "":
        # ssh_client.run("rm /mnt/hdfs/datanode/current/VERSION")
        ssh_client.run("sudo rm /mnt/hdfs/datanode/current/VERSION")


def rsync_folder(ssh_client, slave):
    """

    :param ssh_client:
    :param slave:
    :return:
    """
    with open_cfg() as cfg:
        delete_hdfs = cfg.getboolean('main', 'delete_hdfs')
    ssh_client.run(
        "eval `ssh-agent -s` && ssh-add " + "$HOME/" + PRIVATE_KEY_NAME + " && rsync -a " + HADOOP_CONF + " ubuntu@" + slave + ":" + HADOOP_CONF)
    if delete_hdfs:
        # ssh_client.run("rm /mnt/hdfs/datanode/current/VERSION")
        ssh_client.run("sudo rm /mnt/hdfs/datanode/current/VERSION")


@timing
def setup_hdfs_config(master_node, slaves, hdfs_master):
    """

    :param master_node:
    :param slaves:
    :return:
    """
    with open_cfg() as cfg:
        delete_hdfs = cfg.getboolean('main', 'delete_hdfs')
    ssh_client = sshclient_from_node(master_node, ssh_key_file=PRIVATE_KEY_PATH, user_name='ubuntu')

    if hdfs_master == "":
        master_ip = get_ip(master_node)
    else:
        master_ip = hdfs_master

    # Setup Config HDFS
    ssh_client.run(
        "sed -i '19s%.*%<configuration>  <property>    <name>fs.default.name</name>    <value>hdfs://" + master_ip + ":9000</value>  </property>%g' " + HADOOP_CONF + "core-site.xml")
    # 19 <configuration>  <property>    <name>fs.default.name</name>    <value>hdfs://ec2-54-70-105-139.us-west-2.compute.amazonaws.com:9000</value>  </property>

    ssh_client.run(
        "sed -i '38s%.*%<value>" + master_ip + ":50070</value>%g' " + HADOOP_CONF + "hdfs-site.xml")
    ssh_client.run(
        "sed -i '43s%.*%<value>" + master_ip + ":50090</value>%g' " + HADOOP_CONF + "hdfs-site.xml")
    ssh_client.run(
        "sed -i '48s%.*%<value>" + master_ip + ":9000</value>%g' " + HADOOP_CONF + "hdfs-site.xml")
    # 38  <value>ec2-54-70-105-139.us-west-2.compute.amazonaws.com:50070</value>
    # 43   <value>ec2-54-70-105-139.us-west-2.compute.amazonaws.com:50090</value>
    # 48  <value>ec2-54-70-105-139.us-west-2.compute.amazonaws.com:9000</value>

    ssh_client.run(
        "sed -i 's%/var/lib/hadoop/hdfs/namenode%/mnt/hdfs/namenode%g' " + HADOOP_CONF + "hdfs-site.xml")
    ssh_client.run(
        "sed -i 's%/var/lib/hadoop/hdfs/datanode%/mnt/hdfs/datanode%g' " + HADOOP_CONF + "hdfs-site.xml")

    print(slaves)
    ssh_client.run("echo -e '" + "\n".join(slaves) + "' > " + HADOOP_CONF + "slaves")

    ssh_client.run(
        "echo 'Host *\n  UserKnownHostsFile /dev/null\n  StrictHostKeyChecking no' > ~/.ssh/config")

    # Rsync Config
    with ThreadPoolExecutor(multiprocessing.cpu_count()) as executor:
        for slave in slaves:
            executor.submit(rsync_folder, ssh_client, slave)

    # Start HDFS
    if delete_hdfs or hdfs_master == "":
        ssh_client.run(
            "eval `ssh-agent -s` && ssh-add " + "$HOME/" + PRIVATE_KEY_NAME + " && /usr/local/lib/hadoop-2.7.2/sbin/stop-dfs.sh")
        # ssh_client.run("rm /mnt/hdfs/datanode/current/VERSION")
        ssh_client.run("sudo rm /mnt/hdfs/datanode/current/VERSION")
        ssh_client.run("echo 'N' | /usr/local/lib/hadoop-2.7.2/bin/hdfs namenode -format")

    out, err, status = ssh_client.run(
        "eval `ssh-agent -s` && ssh-add " + "$HOME/" + PRIVATE_KEY_NAME + " && /usr/local/lib/hadoop-2.7.2/sbin/start-dfs.sh && /usr/local/lib/hadoop-2.7.2/bin/hdfs dfsadmin -safemode leave")
    if status != 0:
        print(out, err)
    print("   Started HDFS")

    if delete_hdfs:
        print("   Cleaned HDFS")
        if len(BENCHMARK_PERF) > 0:
            out, err, status = ssh_client.run(
                "/usr/local/lib/hadoop-2.7.2/bin/hadoop fs -rm -R /test/spark-perf-kv-data")
            print(out, err, status)


def write_config(output_folder):
    """

    :param output_folder:
    :return:
    """
    with open(output_folder + "/config.json", "w") as config_out:
        json.dump(CONFIG_DICT, config_out, sort_keys=True, indent=4)


def check_slave_connected_master(ssh_client):
    """

    :param ssh_client:
    :return:
    """
    pass


@timing
def run_benchmark(nodes):
    """

    :return:
    """

    if len(nodes) == 0:
        print("No instances running")
        exit(1)

    with open_cfg(mode='w') as cfg:
        current_cluster = cfg['main']['current_cluster']
        benchmark = cfg['main']['benchmark'] if 'main' in cfg and 'benchmark' in cfg['main'] else ''
        hdfs_master_private_ip = cfg['hdfs']['master_private_ip'] if 'hdfs' in cfg and 'master_private_ip' in cfg['hdfs'] else ''
        hdfs_master_public_ip = cfg['hdfs']['master_public_ip'] if 'hdfs' in cfg and 'master_public_ip' in cfg['hdfs'] else '40.84.226.144'
        delete_hdfs = cfg.getboolean('main', 'delete_hdfs')
        max_executors = int(cfg['main']['max_executors']) if 'main' in cfg and 'max_executors' in cfg['main'] else len(nodes) - 1
        # print('HDFS_MASTER from clusters.ini: ' + hdfs_master)
        end_index = min(len(nodes), max_executors + 1)
        cfg['main']['max_executors'] = str(end_index - 1)
        # TODO: pass slaves ip
        slaves_ip = [get_ip(i) for i in nodes[1:end_index]]

    master_ip, master_node = setup_master(nodes[0], slaves_ip, hdfs_master_private_ip)
    if SPARK_HOME == SPARK_2_HOME:
        print("Check Effectively Executor Running")

    count = 1
    with ThreadPoolExecutor(8) as executor:
        for i in nodes[1:end_index]:
            ip = get_ip(i)
            if ip != master_ip:
                executor.submit(setup_slave, i, master_ip, count)
            count += 1

    with ThreadPoolExecutor(8) as executor:
        for i in nodes[end_index:]:
            ip = get_ip(i)
            if ip != master_ip:
                ssh_client = sshclient_from_node(i, ssh_key_file=PRIVATE_KEY_PATH, user_name='ubuntu')

                executor.submit(common_setup, ssh_client)

    if current_cluster == 'hdfs' or hdfs_master_private_ip == master_ip:
        print("\nStarting Setup of HDFS cluster")
        # Format instance store SSD for hdfs usage
        with ThreadPoolExecutor(multiprocessing.cpu_count()) as executor:
            for i in nodes:
                executor.submit(setup_hdfs_ssd, i, hdfs_master_private_ip)

        slaves = [get_ip(i) for i in nodes[:end_index]]
        slaves.remove(master_ip)
        setup_hdfs_config(master_node, slaves, hdfs_master_private_ip)
        with open_cfg(mode='w') as cfg:
            count = 1
            for ip in slaves:
                cfg['hdfs']['slave'+str(count)+'_ip'] = ip
                count += 1

    time.sleep(15)

    print("MASTER: " + master_ip)
    ssh_client = sshclient_from_node(master_node, PRIVATE_KEY_PATH, user_name='ubuntu')

    #  CHECK IF KEY IN MASTER

    # SPOSTATO IN MASTER_SETUP
    # out, err, status = ssh_client.run('[ ! -e %s ]; echo $?' % PRIVATE_KEY_NAME)
    # if not int(status):
    # files = ssh_client.listdir("/home/ubuntu/")
    # if not PRIVATE_KEY_NAME in files:
    #     ssh_client.put(localpath=PRIVATE_KEY_PATH, remotepath="/home/ubuntu/" + PRIVATE_KEY_NAME)
    #     ssh_client.run("chmod 400 " + "$HOME/" + PRIVATE_KEY_NAME)

    # LANCIARE BENCHMARK
    if current_cluster == 'spark':
        if benchmark == 'pagerank':
            BENCH_CONF["PageRank"]["numV"] = literal_eval(cfg['pagerank']['num_v'])
            num_partitions = cfg['pagerank']['num_partitions']
            BENCH_CONF["PageRank"]["NUM_OF_PARTITIONS"] = (3, num_partitions)
            print('setting numV as {}'.format(BENCH_CONF["PageRank"]["numV"]))
            print('setting NUM_OF_PARTITIONS as {}'.format(BENCH_CONF["PageRank"]["NUM_OF_PARTITIONS"]))

        if benchmark == 'kmeans':
            BENCH_CONF["KMeans"]["NUM_OF_POINTS"] = literal_eval(cfg['kmeans']['num_of_points'])
            num_partitions = cfg['kmeans']['num_partitions']
            BENCH_CONF["KMeans"]["NUM_OF_PARTITIONS"] = (6, num_partitions)
            print('setting NUM_OF_POINTS as {}'.format(BENCH_CONF["KMeans"]["NUM_OF_POINTS"]))
            print('setting NUM_OF_PARTITIONS as {}'.format(BENCH_CONF["KMeans"]["NUM_OF_PARTITIONS"]))

        if len(BENCHMARK_PERF) > 0:
            if delete_hdfs:
                print("   Cleaning HDFS...")
                print("connecting to hdfs_master:{}".format(hdfs_master_public_ip))
                ssh_client_hdfs = sshclient_from_ip(hdfs_master_public_ip, PRIVATE_KEY_PATH, user_name='ubuntu')
                out, err, status = ssh_client_hdfs.run(
                    "/usr/local/lib/hadoop-2.7.2/bin/hadoop fs -rm -R /test/spark-perf-kv-data")
                print(out, err, status)
            print("Running Benchmark " + str(BENCHMARK_PERF))
            runout, runerr, runstatus = ssh_client.run(
                'export SPARK_HOME="' + SPARK_HOME + '" && ./spark-perf/bin/run')
            print('runout\n{}\nrunerr:\n{}\runstatus:{}'.format(runout, runerr, runstatus))

            # FIND APP LOG FOLDER
            print("Finding log folder")
            app_log = between(runout, "2>> ", ".err")
            logfolder = "/home/ubuntu/" + "/".join(app_log.split("/")[:-1])
            print(logfolder)
            out_folder_exp_set = os.path.join(logfolder[1:], cfg['main']['exp_set_name'])

        for bench in BENCHMARK_BENCH:


            for bc in BENCH_CONF[bench]:
                if bc != "NumTrials":
                    ssh_client.run(
                        "sed -i '{0}s/.*/{1}={2}/' ./spark-bench/{3}/conf/env.sh""".format(
                            BENCH_CONF[bench][bc][0], bc, BENCH_CONF[bench][bc][1],
                            bench))

            if delete_hdfs:
                print("Generating Data Benchmark " + bench)
                ssh_client.run(
                    'eval `ssh-agent -s` && ssh-add ' + "$HOME/" + PRIVATE_KEY_NAME + ' && export SPARK_HOME="' + SPARK_HOME + '" && ./spark-bench/' + bench + '/bin/gen_data.sh')

            check_slave_connected_master(ssh_client)
            print("Running Benchmark " + bench)

            ssh_client.run(
                'eval `ssh-agent -s` && ssh-add ' + "$HOME/" + PRIVATE_KEY_NAME + ' && export SPARK_HOME="' + SPARK_HOME + '" && ./spark-bench/' + bench + '/bin/run.sh')
            logfolder = "/home/ubuntu/spark-bench/num"
            out_folder_exp_set = os.path.join('home', 'ubuntu', 'spark-bench', 'num', cfg['main']['exp_set_name'])

        # TODO: DOWNLOAD LOGS
        out_folder_single_exp = log.download(logfolder, [i for i in nodes[:end_index]], master_ip,
                                             out_folder_exp_set, CONFIG_DICT)

        write_config(out_folder_single_exp)
        print("Saving output folder {}".format(os.path.abspath(out_folder_single_exp)))
        cfg['out_folders']['output_folder_'+str(len(cfg['out_folders']))] = os.path.abspath(out_folder_single_exp)
        # Saving cfg on project home directory and output folder
        write_cfg(cfg)
        write_cfg(cfg, out_folder_single_exp)

        # PLOT LOGS
#        plot.plot(output_folder + "/")

        # COMPUTE METRICS
#        metrics.compute_metrics(output_folder + "/")

        print("\nCHECK VALUE OF SCALE FACTOR AND PREV SCALE FACTOR FOR HDFS CASE")
