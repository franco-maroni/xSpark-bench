import copy
import time
from drivers.ccglibcloud.ec2spot import set_spot_drivers
from drivers.azurearm.driver import set_azurearm_driver
from libcloud.compute.providers import get_driver

import launch
import run as x_run
from colors import header, okblue, okgreen, warning, underline, bold, fail

from credentials import AWS_ACCESS_ID, AWS_SECRET_KEY,\
    AZ_APPLICATION_ID, AZ_SECRET, AZ_SUBSCRIPTION_ID, AZ_TENANT_ID
from config import CONFIG_DICT, PROVIDER, REGION, TAG, NUM_INSTANCE, NUM_RUN, CLUSTER_ID, TERMINATE, RUN, REBOOT, CLUSTER_MAP
import util.utils as utils
from spark_log_profiling import processing as profiling
from spark_time_analysis import run as run_ta

class BenchInstance(object):
    driver = None
    nodes = None
    provider_id = None
    cluster_id = None

    def create_nodes(self, num_instances, assume_yes):
        # nodes is a list of "libcloud.compute.base.Node"
        raise NotImplementedError()

    def tag_nodes(self):
        raise NotImplementedError()

    def retrieve_nodes(self):
        raise NotImplementedError()

    def reboot(self):
        print("Rebooting instances...")
        self.retrieve_nodes()
        # Reboot nodes
        for node in self.nodes:
            print("rebooting node {}".format(node.name))
            self.driver.reboot_node(node)
        instance_ids = [n.id for n in self.nodes]
        # Wait for all the nodes to be pingable
        launch.wait_ping_libcloud(self.driver, instance_ids, copy.deepcopy(instance_ids))

    def run(self, num_run):
        with utils.open_cfg(mode='w') as cfg:
            cfg['out_folders'] = {}
            cfg['main']['delete_hdfs'] = 'true'
        for i in range(num_run):
            if self.cluster_id == CLUSTER_MAP['spark']:
                print(bold('Experiment ({}/{})'.format(i + 1, num_run)))
            try:
                self.retrieve_nodes()
                x_run.run_benchmark(self.nodes)
                if i == 0:
                    with utils.open_cfg(mode='w') as cfg:
                        cfg['main']['delete_hdfs'] = 'false'
            except (OSError, IOError) as exc:
                print('ERROR: {}\n\nSkipping Experiment ({}/{})'.format(exc, i + 1, num_run))

    def terminate(self):
        print("Begin termination of instances and cleaning")
        '''
        # Cancel Spot Request
        if PROVIDER == "AWS_SPOT" and num_instance > 0:
            for s in spot_requests:
                driver.ex_cancel_spot_instance_request(s)
            print("Spot requests cancelled")

        ###################################################
        '''
        # Retrieve running nodes
        self.retrieve_nodes()
        # Destroy all nodes
        print("Destroying nodes")
        for node in self.nodes:
            self.driver.destroy_node(node)
            print("node {} destroyed".format(node.name))
        print(okgreen("All nodes destroyed"))

    def setup(self, num_instances, assume_yes):
        # TODO: instead of boolean, raise exception/terminate
        if num_instances > 0:
            # Create nodes
            self.create_nodes(num_instances, assume_yes)
            # nodes is a list of "libcloud.compute.base.Node"
            print("CHECK SECURITY GROUP ALLOWED IP SETTINGS!!!")
            self.tag_nodes()

            instance_ids = [n.id for n in self.nodes]

            # Wait for all the nodes to become RUNNNING
            print("Waiting for nodes to run")
            if launch.wait_for_running_libcloud(self.driver, instance_ids, copy.deepcopy(instance_ids)):
                time.sleep(15)

                # Wait for all the nodes to be pingable
                print("Waiting for nodes to be pingable")
                launch.wait_ping_libcloud(self.driver, instance_ids, copy.deepcopy(instance_ids))
                return True
            else:
                return False
        else:
            return True


class AwsBenchInstace(BenchInstance):
    """
    Aws implementation of bench instance
    NOT TESTED YET
    """
    provider_id = 'AWS_SPOT'

    def __init__(self, cluster_id):
        self.cluster_id = cluster_id
        set_spot_drivers()
        self.cls = get_driver("ec2_spot_" + REGION.replace('-', '_'))
        self.driver = self.cls(AWS_ACCESS_ID, AWS_SECRET_KEY)
        self.spot_requests = None

    def create_nodes(self, num_instances, assume_yes):
        self.nodes, self.spot_requests = launch.launch_libcloud(self.driver, num_instances, CONFIG_DICT,
                                                                self.cluster_id, assume_yes)

    def tag_nodes(self):
        for node in self.nodes:
            self.driver.ex_create_tags(node, TAG[0])

    def retrieve_nodes(self):
        all_nodes = self.driver.list_nodes(ex_filters={'instance-state-name': ['running']})
        self.nodes = [n for n in all_nodes if self.driver.ex_describe_tags(n)['Value'] == self.cluster_id]
        print("Found {} nodes".format(len(self.nodes)))
        return self.nodes


class AzureBenchInstance(BenchInstance):
    """
    AZURE implementation of BenchInstance
    """
    provider_id = 'AZURE'

    def __init__(self, cluster_id):
        self.cluster_id = cluster_id
        set_azurearm_driver()
        self.cls = get_driver("CustomAzureArm")
        self.driver = self.cls(tenant_id=AZ_TENANT_ID,
                               subscription_id=AZ_SUBSCRIPTION_ID,
                               key=AZ_APPLICATION_ID, secret=AZ_SECRET, region=CONFIG_DICT["Azure"]["Location"])

    def create_nodes(self, num_instances, assume_yes):
        self.nodes = launch.launch_libcloud(self.driver, num_instances, CONFIG_DICT, self.cluster_id, assume_yes)

    def tag_nodes(self):
        for node in self.nodes:
            self.driver.ex_create_tags(node, {"ClusterId": self.cluster_id})  # was CONFIG_DICT["Azure"]["ClusterId"]

    def retrieve_nodes(self):
        print("Retrieving nodes of {} cluster".format(self.cluster_id))
        all_nodes = self.driver.list_nodes(ex_resource_group=CONFIG_DICT["Azure"]["ResourceGroup"])
        self.nodes = [n for n in all_nodes if n.extra["tags"]["ClusterId"] == self.cluster_id]
        print("Found {} nodes".format(len(self.nodes)))
        return self.nodes
