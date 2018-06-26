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
def get_withdraw_indexes(df):
    df_type = df['TYPE']

    updates_withdraw_indexes = []

    for i in range(len(df_type)):
        if df_type[i] == 'W':
            updates_withdraw_indexes.append(i)
    print "Find {} W updates".format(len(updates_withdraw_indexes))

    return updates_withdraw_indexes


# Delete W updates and reset index for resulting DataFrame
def delete_withdraw_updates(df):
    updates_withdraw_indexes = get_withdraw_indexes(df)

    df_advises_updates = df
    df_advises_updates = df_advises_updates.drop(df.index[updates_withdraw_indexes])
    df_advises_updates = df_advises_updates.reset_index()
    df_advises_updates = df_advises_updates.drop(['index'], axis=1)

    return df_advises_updates


def get_IPv_type_indexes(df):
    df_prefix = df['PREFIX']

    ipv6_updates_indexes = []
    ipv4_updates_indexes = []

    for i in range(len(df_prefix)):
        # IPv6 filter
        if ':' in df_prefix[i]:
            ipv6_updates_indexes.append(i)
        # IPv4 filter
        else:
            ipv4_updates_indexes.append(i)

    return ipv4_updates_indexes, ipv6_updates_indexes


def separate_IPv_types(df):
    ipv4_updates_indexes, ipv6_updates_indexes = get_IPv_type_indexes(df)

    # Delete corresponding entries
    df_ipv6_updates = df.drop(df.index[ipv4_updates_indexes])
    df_ipv4_updates = df.drop(df.index[ipv6_updates_indexes])

    # Reset indexes
    df_ipv6_updates = df_ipv6_updates.reset_index()
    df_ipv6_updates = df_ipv6_updates.drop(['index'], axis=1)

    df_ipv4_updates = df_ipv4_updates.reset_index()
    df_ipv4_updates = df_ipv4_updates.drop(['index'], axis=1)

    return df_ipv4_updates, df_ipv6_updates


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

            #print least_specific
            #print more_specific

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


if __name__ == "__main__":

    print("---------------")
    print("Stage 4: More Specifics Analysis")
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
    step_dir = '/4.more_specifics_analysis'
    exp.per_step_dir(exp_name, step_dir)

    step_dir = '/4.more_specifics_analysis/IPv4'
    exp.per_step_dir(exp_name, step_dir)

    step_dir = '/4.more_specifics_analysis/IPv6'
    exp.per_step_dir(exp_name, step_dir)

    input_file_path = result_directory + exp_name + '/3.data_cleaning/' + collector + '_' + from_date + '-' + to_date + file_ext
    output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + '.xlsx'

    write_flag = f.overwrite_file(output_file_path)

    if write_flag == 1:
        print "Loading " + input_file_path + "..."

        df_clean = f.read_file(file_ext, input_file_path)

        print "Data loaded successfully"

        df_sort = df_clean.sort_values(by=['MONITOR', 'PREFIX'])

        df_sort = df_sort.reset_index(drop=True)
        df_sort = df_sort.drop(['Unnamed: 0'], axis=1)

        print "Deleting withdraw updates..."

        df_advises = delete_withdraw_updates(df_sort)

        print "Splitting {} advises of {} ...".format(len(df_advises), len(df_clean))

        df_IPv4_updates, df_IPv6_updates = separate_IPv_types(df_advises)

        print 'Data separated for analysis'
        print 'Total Updates: {}'.format(len(df_clean))
        print 'Total Advises: {}'.format(len(df_IPv4_updates) + len(df_IPv6_updates))
        print 'IPv4 updates: {}'.format(len(df_IPv4_updates))
        print 'IPv6 updates: {}'.format(len(df_IPv6_updates))

        print 'IPv4 analysis'
        # IPv4 Analysis
        print 'Getting prefixes seen per monitor'
        pref_IPv4 = get_prefixes_seen_per_monitor(df_IPv4_updates)
        print 'Clustering updates'
        least_specifics_per_monitor, more_specifics_per_monitor, intermediates_per_monitor, uniques_per_monitor = cluster_advises_per_monitor(
            pref_IPv4)

        monitors_IPv4, pref_IPv4_count = count_prefixes_per_monitor(pref_IPv4)
        count_more_specifics_per_monitor, more_specific_ratio = count_more_specifics(more_specifics_per_monitor,
                                                                                     monitors_IPv4, pref_IPv4_count)

        #        refactor_dir_IPv4 = group_by_dirIP_mask(pref_IPv4)
        #        more_specifics_count_per_monitor = count_more_specifics(refactor_dir_IPv4)
        #        more_specific_ratio = calculate_more_specific_ratio(more_specifics_count_per_monitor, pref_IPv4_count)
        #
        step_dir = '/4.more_specifics_analysis/IPv4'
        output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + file_ext
        # Prefix count by mask generated dinamically
        # for monitor in monitors_IPv4:

        df_results_count = pd.DataFrame({'MONITOR': monitors_IPv4, 'PREF_COUNT': pref_IPv4_count,
                                         'MORE_SPECIFICS_COUNT': count_more_specifics_per_monitor,
                                         'MORE_SPECIFIC_RATIO': more_specific_ratio})
        f.save_file(df_results_count, file_ext, output_file_path)

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

#        print 'IPv6 analysis'
#
#        # IPv6 Analysis
#        pref_IPv6 = get_prefixes_seen_per_monitor(df_IPv6_updates)
#        least_specifics_per_monitor = get_least_specifics_per_monitor(pref_IPv6)
#        
#        monitors_IPv6, pref_IPv6_count = count_prefixes_per_monitor(pref_IPv6)
#        count_more_specifics_per_monitor_IPv6, more_specific_ratio_IPv6 = count_more_specifics(least_specifics_per_monitor, monitors_IPv6, pref_IPv6_count)
#
#        
#        step_dir = '/4.more_specifics_analysis/IPv6'
#        output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + file_ext
#        df_results_count = pd.DataFrame({'MONITOR': monitors_IPv6, 'PREF_COUNT': pref_IPv6_count, 'MORE_SPECIFICS_COUNT': count_more_specifics_per_monitor_IPv6, 'MORE_SPECIFIC_RATIO': more_specific_ratio_IPv6})
#        f.save_file(df_results_count, file_ext, output_file_path)
