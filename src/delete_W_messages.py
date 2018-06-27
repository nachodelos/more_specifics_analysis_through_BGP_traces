#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
This script sorts updates per collector by MONITOR and PREFIX fields
This is a way to separate data in a single file per collector

"""
import experiment_manifest as exp
import file_manager as f


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


if __name__ == "__main__":

    print("---------------")
    print("Stage 4: Delete withdraw messages")
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
    step_dir = '/4.delete_withdraw_messages'
    exp.per_step_dir(exp_name, step_dir)

    input_file_path = result_directory + exp_name + '/3.data_cleaning/' + collector + '_' + from_date + '-' + to_date + file_ext
    output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + file_ext

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

        f.save_file(df_advises, file_ext, output_file_path)
