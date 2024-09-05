# Copyright (c) 2023-2024 Broadcom. All Rights Reserved.
# Broadcom Confidential. The term "Broadcom" refers to Broadcom Inc.
# and/or its subsidiaries.

import specs.lvn_sim_tools_meta
import os
import helpers.env

def simulate_lvn_build(cls):
    o_GetCommands = cls.GetCommands
    o_GetComponentDependencies = getattr(cls, "GetComponentDependencies", None)
    o_GetClusterRequirements = cls.GetClusterRequirements

    def _protectionsEnabled(cluster_requirements, hosttype):
        if type(cluster_requirements) is dict:
            return type(cluster_requirements[hosttype]) is dict \
                and 'default_protections' in cluster_requirements[hosttype].keys()
        return False


    def _GetCommands(self, hosttype):
        lvn_sim_tools_root = '%(gobuild_component_lvn_sim_tools_root)/lvn-sim-scripts'
        env = helpers.env.SafeEnvironment(hosttype)
        tcroot = env.get('TCROOT', 'C:/TCROOT-NOT-SET') if hosttype.startswith('windows') else '/build/toolchain'
        extra_protections = {
            'extra_protections': [{
                'root_needed': False,
                'sudoers_file': '%(buildroot)/cayman_antrea/support/gobuild/root/sudoers_for_lvn_simulator',
                'sudoers_rank': 10,
            }]
        }
        common_params = {
            'root': lvn_sim_tools_root,
        }

        if _protectionsEnabled(o_GetClusterRequirements(self), hosttype):
            common_params.update(extra_protections)
        env = {
            'SRC_ROOT': lvn_sim_tools_root,
            'TCROOT': tcroot
        }
        python_cmd = '/build/apps/bin/internal/invoke-build-python'

        lvn_sim_start_cmd = {
            'desc': 'Starting LVN network sim',
            'log': 'start-lvn-sim.log',
            'command': '%s start_lvn_sim.py' % python_cmd,
            'env': env
        }
        lvn_sim_start_cmd.update(common_params)
        lvn_sim_stop_cmd = {
            'desc': 'Stopping LVN network sim',
            'log': 'stop-lvn-sim.log',
            'command': '%s stop_lvn_sim.py' % python_cmd,
            'env': env
        }
        lvn_sim_stop_cmd.update(common_params)
        commands = [lvn_sim_start_cmd]
        commands.extend(o_GetCommands(self, hosttype))
        commands.append(lvn_sim_stop_cmd)

        return commands

    def _GetComponentDependencies(self):
        build_hosts = []
        obj = o_GetClusterRequirements(self)
        if type(obj) is list:
            build_hosts = obj
        elif type(obj) is dict:
            build_hosts = list(obj.keys())

        comps = {}
        if len(build_hosts) > 0:
            hosttypes_to_files = {}
            for hosttype in build_hosts:
                hosttypes_to_files[hosttype] = specs.lvn_sim_tools_meta.LVN_SIM_TOOLS_DELIVERABLE

            comps['lvn-sim-tools'] = {
                'branch': specs.lvn_sim_tools_meta.LVN_SIM_TOOLS_BRANCH,
                'change': specs.lvn_sim_tools_meta.LVN_SIM_TOOLS_CLN,
                'buildtype': specs.lvn_sim_tools_meta.LVN_SIM_TOOLS_BUILD_TYPE,
                'files': hosttypes_to_files
            }
        if o_GetComponentDependencies:
            result = o_GetComponentDependencies(self)
            comps.update(result)
        return comps

    cls.GetCommands = _GetCommands
    cls.GetComponentDependencies = _GetComponentDependencies
    return cls

