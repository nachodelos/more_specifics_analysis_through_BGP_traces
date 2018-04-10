#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
This script dumps data into a proper struct to work more efficiently in Python. In addition you can visualize the corresponding Data with Excel

"""
import subprocess
import pandas as pd
import experiment_manifest as exp
from calendar import monthrange


# FUNCTIONS
def format_number_to_string(num):
    if num < 10:
        num_str = '0' + str(num)
    else:
        num_str = str(num)
    return num_str


def dump_into_lists(update_lines, times, types, s_IPs, s_AS, prefixes, AS_PATHs):
    message = update_lines.split('|')

    m_type = message[2]

    times.append(message[1])
    types.append(m_type)
    s_IPs.append(message[3])
    s_AS.append(message[4])

    if m_type == 'A':
        prefixes.append(message[5])
        AS_PATH_list = message[6].split(' ')
        AS_PATHs.append(AS_PATH_list)
    elif m_type == 'W':
        prefixes.append(message[5])
        AS_PATHs.append([])
    elif m_type == 'STATE':
        prefixes.append('')
        AS_PATHs.append([])


if (__name__ == '__main__'):

    print("---------------")
    print("Stage 1: Load Raw Data")
    print("---------------")

    exp_name, collector = exp.load_arguments()

    experiments = getattr(exp, 'experiments')
    experiment = experiments[exp_name]

    from_date = experiment['initDay']
    to_date = experiment['endDay']
    ris_type = experiment['RISType']

    # VARIABLES (pathlib)
    file_path = '/srv/agarcia/passive_mrai/bgp_updates/' + collector + '/'
    # bgpdump_path = '/srv/alutu/bgpdump/bgpdump'
    bgpdump_path = '/usr/local/bin/bgpdump'
    output_file_path = '/srv/agarcia/igutierrez/results/' + exp_name + '/1.load_data/' + collector + '_' + from_date + '-' + to_date + '.xlsx'

    if (ris_type == 'rrc'):
        hop_size = 5

    from_min = int(from_date.split('.')[1][2:4])
    to_min = int(to_date.split('.')[1][2:4])

    from_hour = int(from_date.split('.')[1][0:2])
    to_hour = int(to_date.split('.')[1][0:2])

    from_day = int(from_date.split('.')[0][6:8])
    to_day = int(to_date.split('.')[0][6:8])

    from_month = int(from_date.split('.')[0][4:6])
    to_month = int(to_date.split('.')[0][4:6])

    from_year = int(from_date.split('.')[0][0:4])
    to_year = int(to_date.split('.')[0][0:4])

    print(from_date + ' ' + to_date)

    update_lines = []

    for month in range(from_month, to_month + 1, 1):

        month_str = format_number_to_string(month)

        if from_month == to_month:
            from_day_aux = from_day
            to_day_aux = to_day
        elif to_month > from_month:
            if month == from_month:
                from_day_aux = from_month
                to_day_aux = monthrange(from_year, from_month)[1]
            elif from_month < month < to_month:
                from_day_aux = 1
                to_day_aux = monthrange(from_year, from_month)[1]
            elif month == to_month:
                from_day_aux = 1
                to_day_aux = to_day

        for dd in range(from_day, to_day + 1, 1):

            dd_str = format_number_to_string(dd)

            if from_day == to_day:
                from_hour_aux = from_hour
                to_hour_aux = to_hour
            elif to_day > from_day:
                if dd == from_day:
                    from_hour_aux = from_hour
                    to_hour_aux = 23
                elif from_day < dd < to_day:
                    from_hour_aux = 0
                    to_hour_aux = 0
                elif dd == to_day:
                    from_hour_aux = 0
                    to_hour_aux = to_hour

            for hh in range(from_hour_aux, to_hour_aux + 1, 1):

                hh_str = format_number_to_string(hh)

                if from_hour == to_hour:
                    from_min_aux = from_min
                    to_min_aux = to_min
                else:
                    if hh == from_hour and dd == from_day:
                        from_min_aux = from_min
                        to_min_aux = 56
                    elif hh == to_hour and dd == to_day:
                        from_min_aux = 0
                        to_min_aux = to_min
                    else:
                        from_min_aux = 0
                        to_min_aux = 56

                for mm in range(from_min_aux, to_min_aux + 1, 5):
                    mm_str = format_number_to_string(mm)
                    print(file_path + 'updates.201801' + dd_str + '.' + hh_str + mm_str)
                    update_lines += subprocess.check_output(
                        [bgpdump_path, '-m', file_path + 'updates.201801' + dd_str + '.' + hh_str + mm_str]).strip().split(
                        '\n')

    # DATA FIELDS
    times = []
    types = []
    s_IPs = []
    s_AS = []
    prefixes = []
    AS_PATHs = []

    # dump data into several lists
    for i in range(len(update_lines)):
        dump_into_lists(update_lines[i], times, types, s_IPs, s_AS, prefixes, AS_PATHs)

    print (' Data saved as lists!')
    df_update = pd.DataFrame(
        {'TIME': times, 'TYPE': types, 'MONITOR': s_IPs, 'AS': s_AS, 'PREFIX': prefixes, 'AS_PATH': AS_PATHs})
    print (' Data Frame created!')
    writer = pd.ExcelWriter(output_file_path, engine='xlsxwriter')
    df_update.to_excel(writer, sheet_name='Sheet1', index=False)
    writer.save()
    print(' Excel File saved!')
