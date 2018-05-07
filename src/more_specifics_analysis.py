#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""

This script analyses several more specifics features from captured data from any collector. It is the following step to the preprocessing.

"""

import experiment_manifest as exp
import file_manager as f


def get_withdraw_indexes(df):
    df_type = df['TYPE']

    updates_withdraw_indexes = []

    for i in range(len(df)):
        if df_type[i] == 'W':
            updates_withdraw_indexes.append(i)
    print "Encontrados {} W updates".format(len(updates_withdraw_indexes))

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

    step_dir = '/4.more_specifics_analysis'
    exp.per_step_dir(exp_name, step_dir)

    input_file_path = result_directory + exp_name + '/3.data_cleaning/' + collector + '_' + from_date + '-' + to_date + file_ext
    output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + file_ext

    write_flag = f.overwrite_file(output_file_path)

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

        pref_IPv4 = get_prefixes_seen_per_monitor(df_IPv4_updates)
        pref_IPv6 = get_prefixes_seen_per_monitor(df_IPv6_updates)


