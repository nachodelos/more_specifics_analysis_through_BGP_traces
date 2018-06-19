#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
This script cleans data following several recomendations of the article "Quantifying path exploration in the Internet"

"""

import experiment_manifest as exp
import file_manager as f


# FUNCTIONS
def get_state_indexes(types):
    indexes = []
    for i, m_type in enumerate(types):
        if m_type == 'STATE':
            indexes.append(i)
    return indexes


def get_affected_message_indexes_per_STATE(state_index, monitors, types, times):

    central_time = int(times[state_index])
    initial_time = central_time - 5
    final_time = central_time + 5
    monitor = monitors[state_index]

    i = state_index
    backward_affected_indexes = []  

    while i - 1 >= 0:
        i = i - 1
        if types[i] != 'STATE' and monitors[i] == monitor and int(times[i]) >= initial_time:
            backward_affected_indexes.append(i)


    i = state_index
    forward_affected_indexes = []

    while i + 1 < len(monitors):
        i = i + 1
        if types[i] != 'STATE' and monitors[i] == monitor and int(times[i]) <= final_time:
            forward_affected_indexes.append(i)      

    affected_indexes = backward_affected_indexes[::-1] + [state_index] + forward_affected_indexes

    return affected_indexes


if __name__ == '__main__':

    print("---------------")
    print("Stage 3: Cleaning updates")
    print("---------------")

    # VARIABLES (experiment)
    exp_name, collector = exp.load_arguments()

    experiments = getattr(exp, 'experiments')
    experiment = experiments[exp_name]

    from_date = experiment['initDay']
    to_date = experiment['endDay']
    result_directory = experiment['resultDirectory']
    file_ext = experiment['resultFormat']

    step_dir = '/3.data_cleaning'
    exp.per_step_dir(exp_name, step_dir)

    input_file_path = result_directory + exp_name + '/2.sort_data_for_cleaning/' + collector + '_' + from_date + '-' + to_date + file_ext
    output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + file_ext

    write_flag = f.overwrite_file(output_file_path)

    if write_flag == 1:

        print ('Loading ' + input_file_path + '...')

        df = f.read_file(file_ext, input_file_path)

        print('Data loaded successfully')

        print('\nConverting timestamp to minutes...\n')

        df_time_s = df['TIME']
        df_time_mm = df_time_s // 60
        df_time_list = df_time_mm.tolist()

        df_type = df['TYPE']
        df_type_list = df_type.tolist()

        state_indexes = get_state_indexes(df_type_list)
        print (len(state_indexes))

        df_monitor = df['MONITOR']
        df_monitor_list = df_monitor.tolist()

        affected_messages = []

        print ('\nSearching affected messages...')

        df_clean = df
        affected_indexes = []

        for i in reversed(state_indexes):
            affected_indexes += get_affected_message_indexes_per_STATE(i, df_monitor_list, df_type_list, df_time_list)

        df_clean = df_clean.drop(df.index[affected_indexes])
        df_clean = df_clean.reset_index(drop=True)  
        df_clean = df_clean.drop(['Unnamed: 0'],axis=1)

        f.save_file(df_clean, file_ext, output_file_path)

        print ('Clean data saved')
