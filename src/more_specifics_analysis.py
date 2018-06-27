#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""

This script analyses several more specifics features from captured data from any collector. It is the following step to the preprocessing.

"""

import experiment_manifest as exp
import file_manager as f
import pandas as pd
from ipaddress import ip_network


# FUNCTIONS
def get_prefixes_seen_per_monitor(df):
    prefixes_per_monitor = {}
    df_monitor = df['MONITOR']
    df_prefix = df['PREFIX']

    for i in range(len(df['MONITOR'])):
        if df_monitor[i] not in prefixes_per_monitor:
            prefixes_per_monitor[df_monitor[i]] = [df_prefix[i]]
        else:
            if df_prefix[i] not in prefixes_per_monitor[df_monitor[i]]:
                prefixes_per_monitor[df_monitor[i]].append(df_prefix[i])

    return prefixes_per_monitor


def count_prefixes_per_monitor(dic):
    monitors = []
    count_pref_per_monitor = []

    for monitor in dic:
        monitors.append(monitor)
        count_pref_per_monitor.append(len(dic[monitor]))

    return monitors, count_pref_per_monitor


def cluster_advises_per_monitor(dic):
    least_specifics = {}
    more_specifics = {}
    intermediates = {}
    uniques = {}

    print 'Collector with {} monitors'.format(len(dic))

    for monitor in dic:
        print monitor
        least_specifics_per_monitor = []
        more_specifics_per_monitor = []
        intermediates_per_monitor = []
        uniques_per_monitor = []
        # Convert every prefix to ip_network object
        # This object allows prefix matching
        pref_IP_network = [ip_network(unicode(pref)) for pref in dic[monitor]]
        # Cluster each prefix
        for prefix_candidate in dic[monitor]:

            prefix_candidate = ip_network(unicode(prefix_candidate))

            least_specific = [pref_to_match for pref_to_match in pref_IP_network if prefix_candidate.overlaps(
                pref_to_match) and prefix_candidate.netmask > pref_to_match.netmask]
            more_specific = [pref_to_match for pref_to_match in pref_IP_network if prefix_candidate.overlaps(
                pref_to_match) and prefix_candidate.netmask < pref_to_match.netmask]

            if len(least_specific) == 0 and len(more_specific) != 0:
                least_specifics_per_monitor.append(prefix_candidate.with_prefixlen)
            elif len(more_specific) == 0 and len(least_specific) != 0:
                more_specifics_per_monitor.append(prefix_candidate.with_prefixlen)
            elif len(more_specific) != 0 and len(least_specific) != 0:
                intermediates_per_monitor.append(prefix_candidate.with_prefixlen)
            else:
                uniques_per_monitor.append(prefix_candidate.with_prefixlen)

        least_specifics[monitor] = least_specifics_per_monitor
        more_specifics[monitor] = more_specifics_per_monitor
        intermediates[monitor] = intermediates_per_monitor
        uniques[monitor] = uniques_per_monitor

    return least_specifics, more_specifics, intermediates, uniques


def group_by_dirIP_mask(dic):
    reformat_dic = {}

    for monitor in dic:
        dir_ip_per_monitor = {}
        for pref in dic[monitor]:
            splitted_pref = pref.split('/')
            dir_IP = splitted_pref[0]
            mask = splitted_pref[1]

            if pref not in dir_ip_per_monitor:
                dir_ip_per_monitor[dir_IP] = [mask]
            else:
                dir_ip_per_monitor[dir_IP].append(mask)

        reformat_dic[monitor] = dir_ip_per_monitor

    return reformat_dic


def count_more_specifics(dic, monitors, number_of_prefixes):
    more_specifics_per_monitor = []
    more_specific_ratio = []

    for i, monitor in enumerate(monitors):
        more_specific_per_monitor_count = len(dic[monitor])
        more_specifics_per_monitor.append(more_specific_per_monitor_count)
        more_specific_ratio.append(float(more_specific_per_monitor_count) / float(number_of_prefixes[i]))

    return more_specifics_per_monitor, more_specific_ratio


def count_more_specifics_by_mask(dic, monitors):
    more_specifics_by_mask_per_monitor = {}

    for i, monitor in enumerate(monitors):
        for prefix in dic[monitor]:
            mask = prefix.split('/')[1]
            if mask in more_specifics_by_mask_per_monitor[monitor]:
                count_per_mask = more_specifics_by_mask_per_monitor[monitor][mask]
                more_specifics_by_mask_per_monitor[monitor][mask] = count_per_mask + 1
            else:
                more_specifics_by_mask_per_monitor[monitor][mask] = 1

    return more_specifics_by_mask_per_monitor


def calculate_more_specific_ratio(ms_count, pr_count):
    more_specific_ratio = []

    for i in range(len(ms_count)):
        more_specific_ratio.append(float(ms_count[i]) / float(pr_count[i]))

    return more_specific_ratio


def generate_lists_for_dataframe(dic, p_type):
    monitors = []
    prefixes = []
    prefix_type = []

    for monitor in dic:
        for prefix in dic[monitor]:
            monitors.append(monitor)
            prefixes.append(prefix)
            prefix_type.append(p_type)

    return monitors, prefixes, prefix_type


def IPv_analysis(IPv_type, exp_n, s_dir, res_directory, coll, from_d, to_d, ext):
    input_file_path = res_directory + exp_n + '/5.split_data_for_analysis/' + IPv_type + '/' + coll + '_' + from_d + '-' + to_d + ext
    output_file_path = res_directory + exp_n + s_dir + IPv_type + '/' + collector + '_' + from_d + '-' + to_d + '.xlsx'

    write_flag = f.overwrite_file(output_file_path)

    if write_flag == 1:
        print "Loading " + input_file_path + "..."

        df_updates = f.read_file(file_ext, input_file_path)

        print "Data loaded successfully"
        # IPv4 Analysis
        print 'Getting prefixes seen per monitor'
        prefs = get_prefixes_seen_per_monitor(df_updates)
        print 'Clustering updates'
        least_specifics_per_monitor, more_specifics_per_monitor, intermediates_per_monitor, uniques_per_monitor = cluster_advises_per_monitor(
            prefs)

        monitors, pref_count = count_prefixes_per_monitor(prefs)
        count_more_specifics_per_monitor, more_specific_ratio = count_more_specifics(more_specifics_per_monitor,
                                                                                     monitors, pref_count)

        step_dir = '/6.more_specifics_analysis/' + IPv_type
        output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + '.xlsx'
        # Prefix count by mask generated dinamically
        # for monitor in monitors_IPv4:

        df_results_count = pd.DataFrame({'MONITOR': monitors, 'PREF_COUNT': pref_count,
                                         'MORE_SPECIFICS_COUNT': count_more_specifics_per_monitor,
                                         'MORE_SPECIFIC_RATIO': more_specific_ratio})
        f.save_file(df_results_count, '.xlsx', output_file_path)

        more_specific_monitors, more_specific_prefixes, more_specific_types = generate_lists_for_dataframe(
            more_specifics_per_monitor, 'more_specific')
        intermediate_monitors, intermediate_prefixes, intermediate_types = generate_lists_for_dataframe(
            intermediates_per_monitor, 'intermediate')
        least_specific_monitors, least_specific_prefixes, least_specific_types = generate_lists_for_dataframe(
            least_specifics_per_monitor, 'least_specific')
        unique_monitors, unique_prefixes, unique_types = generate_lists_for_dataframe(uniques_per_monitor, 'uniques')

        monitors = more_specific_monitors + intermediate_monitors + least_specific_monitors + unique_monitors
        prefixes = more_specific_prefixes + intermediate_prefixes + least_specific_prefixes + unique_prefixes
        types = more_specific_types + intermediate_types + least_specific_types + unique_types

        df_clustering = pd.DataFrame({'MONITOR': monitors, 'PREFIX': prefixes, 'TYPE': types})
        output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + '_clustering' + '.xlsx'
        f.save_file(df_clustering, '.xlsx', output_file_path)


if __name__ == "__main__":
    print("---------------")
    print("Stage 6: More Specifics Analysis")
    print("---------------")

    # VARIABLES (experiment)
    exp_name, collector = exp.load_arguments()

    experiments = getattr(exp, 'experiments')
    experiment = experiments[exp_name]

    from_date = experiment['initDay']
    to_date = experiment['endDay']
    result_directory = experiment['resultDirectory']
    file_ext = experiment['resultFormat']

    # Directories creation
    step_dir = '/6.more_specifics_analysis'
    exp.per_step_dir(exp_name, step_dir)

    step_dir = '/6.more_specifics_analysis/IPv4/'
    exp.per_step_dir(exp_name, step_dir)

    step_dir = '/6.more_specifics_analysis/IPv6/'
    exp.per_step_dir(exp_name, step_dir)

    # IPv4 analysis
    IPv_analysis('IPv4', exp_name, step_dir, result_directory, collector, from_date, to_date, file_ext)

    # IPv6 analysis
    IPv_analysis('IPv6', exp_name, step_dir, result_directory, collector, from_date, to_date, file_ext)
