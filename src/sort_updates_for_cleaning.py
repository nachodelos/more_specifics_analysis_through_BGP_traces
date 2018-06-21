#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
This script sorts updates per collector by MONITOR and TIME fields
This is a way to separate data in a single file per collector

"""
import experiment_manifest as exp
import file_manager as f
import os


def check_output(input_file_path, output_file_path):
    if os.path.getsize(input_file_path) == os.path.getsize(output_file_path):
        print "File created properly"
    else:
        print "WARNING FILE WITH ERRORS: File output size does't match with file input size."


if __name__ == '__main__':

    print("---------------")
    print("Stage 2: Sort updates for cleaning")
    print("---------------")

    # VARIABLES (experiment)
    exp_name, collector = exp.load_arguments()

    experiments = getattr(exp, 'experiments')
    experiment = experiments[exp_name]

    from_date = experiment['initDay']
    to_date = experiment['endDay']
    result_directory = experiment['resultDirectory']
    file_ext = experiment['resultFormat']

    step_dir = '/2.sort_data_for_cleaning'
    exp.per_step_dir(exp_name, step_dir)

    input_file_path = result_directory + exp_name + '/1.load_data/' + collector + '_' + from_date + '-' + to_date + file_ext
    output_file_path = result_directory + exp_name + step_dir + '/' + collector + '_' + from_date + '-' + to_date + file_ext

    write_flag = f.overwrite_file(output_file_path)

    if write_flag == 1:
        print ('Loading ' + input_file_path + ' ...')

        df = f.read_file(file_ext, input_file_path)

        df_sort = df.sort_values(by=['MONITOR', 'TIME'])

        df_sort = df_sort.reset_index(drop=True)
        df_sort = df_sort.drop(['Unnamed: 0'], axis=1)

        f.save_file(df_sort, file_ext, output_file_path)

        check_output(input_file_path, output_file_path)
