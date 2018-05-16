#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""

This script analyses several more specifics features from captured data from any collector. It is the following step to the preprocessing.

"""

import experiment_manifest as exp
import file_manager as f
import pandas as pd


# FUNCTIONS
def get_withdraw_indexes(df):
    df_type = df['TYPE']

    updates_withdraw_indexes = []

    for i in range(len(df)):
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

    for i in range(len(df)):
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

    for i in range(len(df)):
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


def group_by_dirIP_mask(dic):
    reformat_dic = {}

    for monitor in dic:       
        dir_ip_per_monitor = {} # Each dictionary element has the following format {'monitor_1':{'dir_IP_1':['/20', '/21' ...], 'monitor_2': ...}
        for pref in dic[monitor]:
            splitted_pref = pref.split('/')
            dir_IP = splitted_pref[0]
            mask = splitted_pref[1]
            
            if dir_IP not in dir_ip_per_monitor:
                dir_ip_per_monitor[dir_IP] = [mask]
            else:
                dir_ip_per_monitor[dir_IP].append(mask)
        
        reformat_dic[monitor] = dir_ip_per_monitor        
    
    return reformat_dic        


def count_more_specifics(dic):
    count_more_specifics_per_monitor = []
    
    for monitor in dic:
        count_more_specifics = 0
        for dir_IP in dic[monitor]:
            if len(dic[monitor][dir_IP]) > 1:
                count_more_specifics = count_more_specifics + (len(dic[monitor][dir_IP]) - 1)
#            elif len(dic[monitor][dir_IP]) == 1:
#                count_more_specifics = count_more_specifics + len(dic[monitor][dir_IP])
                
        count_more_specifics_per_monitor.append(count_more_specifics)
        
    return count_more_specifics_per_monitor


def calculate_more_specific_ratio(ms_count, pr_count):
    more_specific_ratio = []    

    for i in range(len(ms_count)):
        more_specific_ratio.append(float(ms_count[i])/float(pr_count[i]))
    
    return more_specific_ratio    
    
    
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
    output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + file_ext

    write_flag =  1# f.overwrite_file(output_file_path)

    if write_flag == 1:
        print "Loading " + input_file_path + "..."

        df_clean = f.read_file(file_ext, input_file_path)

        print "Data loaded successfully"
        print "Deleting withdraw updates..."

        df_advises = delete_withdraw_updates(df_clean)

        print "Splitting {} advises of {} ...".format(len(df_advises), len(df_clean))

        df_IPv4_updates, df_IPv6_updates = separate_IPv_types(df_advises)

        print 'Data separated for analysis'
        print 'Total Updates: {}'.format(len(df_clean))
        print 'Total Advises: {}'.format(len(df_IPv4_updates) + len(df_IPv6_updates))
        print 'IPv4 updates: {}'.format(len(df_IPv4_updates))
        print 'IPv6 updates: {}'.format(len(df_IPv6_updates))

        # IPv4 Analysis
        pref_IPv4 = get_prefixes_seen_per_monitor(df_IPv4_updates)
        monitors_IPv4, pref_IPv4_count = count_prefixes_per_monitor(pref_IPv4)
        refactor_dir_IPv4 = group_by_dirIP_mask(pref_IPv4)
        more_specifics_count_per_monitor = count_more_specifics(refactor_dir_IPv4)
        more_specific_ratio = calculate_more_specific_ratio(more_specifics_count_per_monitor, pref_IPv4_count)

        step_dir = '/4.more_specifics_analysis/IPv4'
        output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + file_ext
        df_results_count = pd.DataFrame({'MONITOR': monitors_IPv4, 'PREF_COUNT': pref_IPv4_count, 'MORE_SPECIFICS_COUNT': more_specifics_count_per_monitor, 'MORE_SPECIFIC_RATIO': more_specific_ratio})
        f.save_file(df_results_count, file_ext, output_file_path)

        # IPv6 Analysis
        pref_IPv6 = get_prefixes_seen_per_monitor(df_IPv6_updates)
        monitors_IPv6, pref_IPv6_counts = count_prefixes_per_monitor(pref_IPv6)

        step_dir = '/4.more_specifics_analysis/IPv6'
        output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + file_ext
        df_results_count = pd.DataFrame({'MONITOR': monitors_IPv6, 'PREF_COUNT': pref_IPv6_counts})
        f.save_file(df_results_count, file_ext, output_file_path)
