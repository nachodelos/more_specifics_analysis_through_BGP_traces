#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import experiment_manifest as exp
import file_manager as f


# FUNCTIONS
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


if __name__ == '__main__':
    print("---------------")
    print("Stage 5: Split data for analysis")
    print("---------------")

    # VARIABLES (experiment)
    exp_name, collector = exp.load_arguments()

    experiments = getattr(exp, 'experiments')
    experiment = experiments[exp_name]

    from_date = experiment['initDay']
    to_date = experiment['endDay']
    result_directory = experiment['resultDirectory']
    file_ext = experiment['resultFormat']

    step_dir = '/5.split_data_for_analysis'
    exp.per_step_dir(exp_name, step_dir)

    step_dir = '/5.split_data_for_analysis/IPv4'
    exp.per_step_dir(exp_name, step_dir)

    step_dir = '/5.split_data_for_analysis/IPv6'
    exp.per_step_dir(exp_name, step_dir)

    input_file_path = result_directory + exp_name + '/4.concatenate_RIB_data/' + collector + '_' + from_date + '-' + to_date + file_ext
    output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + file_ext

    write_flag = f.overwrite_file(output_file_path)

    if write_flag == 1:
        print "Loading " + input_file_path + "..."

        df_advises = f.read_file(file_ext, input_file_path)

        print "Data loaded successfully"

        print "Splitting {} advises...".format(len(df_advises))

        df_IPv4_updates, df_IPv6_updates = separate_IPv_types(df_advises)

        df_IPv4_updates = df_IPv4_updates.drop(['Unnamed: 0'], axis=1)
        df_IPv6_updates = df_IPv6_updates.drop(['Unnamed: 0'], axis=1)

        step_dir = '/5.split_data_for_analysis/IPv4'
        output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + file_ext

        f.save_file(df_IPv4_updates, file_ext, output_file_path)

        step_dir = '/5.split_data_for_analysis/IPv6'
        output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + file_ext

        f.save_file(df_IPv6_updates, file_ext, output_file_path)