#!/usr/bin/env python

'''Description and configuration of the experiments to be done'''

from argparse import ArgumentParser

import os
import sys

from time import strftime, gmtime
import calendar

# Not a class, everything can be returned classless - I'm not storing this in another file...
# For now, there are two types of experiments:
# - ris beacon experiments - subdetails for this in ris_beacon_experiment_manifest.py.
# - peering experiments (to be implemented)

# Each experiment is composed of
# - name: '1risDec17', '2peeringDec17', ... With format '#descripDATE', experiment number (to distinguish easily one to the other), description of the experiment, duration Date...
# 	This is a dictionary 'name': rest of information.
# 	Be careful not to remove information of previous meaningful experiments from the file, since this is the place in which I have the details of each experiment.
# - description
# - beacon prefixes involved, anchor prefixes involved (from the same location)
# 	ris_beacons
# 	peering_beacons
# - initDay/endDay in 'YYYYmmdd' format. All experiments are aligned to a day, 00:00:00 of a day.
#   endDay is date for the FIRST DAY without experiment. I.e. endDay > initDay
# - at which time prefixes are advertised/removed
# - results directory, - here is the place in which I set absolute names for the directory
experiments = {
    'experiment_1': {
        'description': 'Developing tests', 
        'RISType': 'rrc',
        'collectors': 'rrc00' 
        'initDay': '20180108.0400', # ts 1509494400,  
        'endDay': '20180108.0410', # 'endTime': 1509667200,      # two days later, Nov 3rd ...
        'resultDirectory': '/srv/agarcia/igutierrez/results/',
    },
    'experiment_2': {
        'description': 'Developing tests', 
        'RISType': 'route_views', 
        'initDay': '20180108.0400', # ts 1509494400,  
        'endDay': '20180108.0410', # 'endTime': 1509667200,      # two days later, Nov 3rd ...
        'resultDirectory': '/srv/agarcia/igutierrez/results/',    
    
    },
    # Put here more experiments, never remove any (to keep the record)
}

# guard_seconds: number of seconds to download in advance before the experiment
GUARD_SECONDS = int(60*10)

# If it is not ok, exists. Otherwise does nothing.
def check_exp_name_ok(expName):
    if type(expName) is not str:
        print('Invalid experiment type, expected str, found {}').format(type(expName))
        exit(1)
    if experiments.get(expName) == None:
        print('Unknown experiment name: {}\n').format(expName)
        exit(1)

# Check if starting time is aligned with 00:00:00
# def _check_exp_starting_time_alignment(expName):
#     # >>> time.gmtime(1509494400)
#     # time.struct_time(tm_year=2017, tm_mon=11, tm_mday=1, tm_hour=0, tm_min=0, tm_sec=0, tm_wday=2, tm_yday=305, tm_isdst=0)
#     starting_time = gmtime(experiments[expName]['initTime'])
#     if (starting_time.tm_hour != 0 or starting_time.tm_min != 0 or starting_time.tm_sec != 0):
#         print('Unexpected initTime alignment, initTime: {} ').format(starting_time)
#         exit(1)
#     if starting_time.tm_year < 2000:
#         print('initTime before 2000, too early')
#         exit(1)

# fullday ~ '20171101'
def day2timestamp(fullday):
    if len(fullday) != 8:
        print('day2timestamp, wrong day format')
        exit(1)

    year = int(fullday[:4])
    month = int(fullday[4:6])
    day = int(fullday[6:8])
    return calendar.timegm((year, month, day,0, 0, 0, 0))

def timestamp2day(timestamp):
    return strftime('%Y%m%d', gmtime(timestamp))
    

# classless methods:
#   experiment_beacons(expName): returns list of beacons - remember, 'beacon' in wide sense.
# can return list of tuples 
#   [('pref1', 'pref2', 'pref3'), ('pref4', 'pref5')]
# This means that the first ANCHOR corresponds to the first tuple returned, and so on
def experiment_beacons(expName):
    check_exp_name_ok(expName) 
    if 'ris_beacons' == experiments[expName]['beaconType']:
        # position [0] is beacon
        return [x[0] for x in ris_beacon_anchor_list]
    # complete with other experiment types

def experiment_anchors(expName):
    check_exp_name_ok(expName) 
    if 'ris_beacons' == experiments[expName]['beaconType']:
        # position [1] is anchor
        return [x[1] for x in ris_beacon_anchor_list]
    # complete with other experiment types

# for risBeacons, it is a single prefix
# for other experiment types, can be a list
def beacon_corresponding_to_anchor(expName, anchor):
    check_exp_name_ok(expName) 
    if 'ris_beacons' == experiments[expName]['beaconType']:
        for pair in ris_beacon_anchor_list:
            if pair[1] == anchor:
                return pair[0]


def beacon_type(expName):
    check_exp_name_ok(expName)
    return experiments[expName]['beaconType']

def experiment_init_day(expName):
    check_exp_name_ok(expName) 
    return experiments[expName]['initDay']

def experiment_end_day(expName):
    check_exp_name_ok(expName) 
    # endDay must be strictly greater than initDay
    if experiments[expName]['endDay'] <= experiments[expName]['initDay']:
        print('experiment_manifest: initDay is equal or higher than endDay, stopping')
        exit(1)
    return experiments[expName]['endDay']


def experiment_number_days(expName):
    check_exp_name_ok(expName) 
    return (day2timestamp(experiments[expName]['endDay']) - day2timestamp(experiments[expName]['initDay']))/(24*60*60)

# returns ORDERED list of UNIX timestamps starting times [78873, 78970, ...].
def experiment_beacon_adv_times(expName): 
    check_exp_name_ok(expName)
    if 'risBeaconsSch' == experiments[expName]['updateSchedule']:
        return ris_beacon_adv_times(day2timestamp(experiments[expName]['initDay']), day2timestamp(experiments[expName]['endDay']))
    # other experiment types

def experiment_beacon_wd_times(expName):
    check_exp_name_ok(expName)
    if 'risBeaconsSch' == experiments[expName]['updateSchedule']:
        return ris_beacon_wd_times(day2timestamp(experiments[expName]['initDay']), day2timestamp(experiments[expName]['endDay']))
    # other experiment types

# Returns (init_interval, end_interval, A/W type)
# [(1509494400, 1509501600, 'A'), (1509501600, 1509508800, 'W'), (1509508800, 1509516000, 'A')...]
def experiment_beacon_adv_wd_times(exp_name): 
    # timestamp_list = experiment_beacon_adv_times(exp_name)
    timestamp_list =[]
    for interval in experiment_beacon_adv_times(exp_name): 
        timestamp_list.append((interval[0], interval[1], 'A'))
    for interval in experiment_beacon_wd_times(exp_name):
        timestamp_list.append((interval[0], interval[1], 'W'))
    return sorted(timestamp_list)
# beacon_experiment_interval(exp_name): [(ts at which beacon is adv, ts at which it is wd), ()]


def _print_experiment_info(expName):
    check_exp_name_ok(expName)
    print('\nExperiment: {}').format(expName) 
    print('  Beacons: {}').format(experiment_beacons(expName))
    print('  Anchors: {}').format(experiment_anchors(expName))
    print('  Starting day (UNIX Timestamp): {}').format(experiment_init_day(expName))
    print('  End day (UNIX Timestamp): {}').format(experiment_end_day(expName))
    print('  ... number of days: {}').format(experiment_number_days(expName))
    print('  Adv times: {}').format(experiment_beacon_adv_times(expName))
    print('  Wd times: {}').format(experiment_beacon_wd_times(expName))
    print('  Ad + Wd times: {}').format(experiment_beacon_adv_wd_times(expName))

# returns list of active collectors at the time of the experiment
def experiment_collectors(expName):
    full_day = experiment_init_day(expName)
    year = int(full_day[:4])
    collector_list=generate_collector_names(year)

    collector_str =''
    #return in string 'route-views.eqix' 'route-views.isc' ..
    for collector in collector_list:
        collector_str += collector + " "
    return collector_str

###
### DIRECTORIES and FILENAMES
###

# This directory must exist beforehand (this is the way to check that the code
# is running in the appropriate system)
# '/srv/agarcia/beacon_mrai/ris_beacons/'
def experiment_base_result_dir(expName):
    check_exp_name_ok(expName)
    target_dir = experiments[expName]['resultDirectory'] + experiments[expName]['beaconType'] + '/' 
    if not os.path.isdir(target_dir):
        raise Exception('Base result directory ' + target_dir + ' did not existed. Check if the system is ok, etc.')
    return target_dir

# '/srv/agarcia/beacon_mrai/ris_beacons/0testDic17/'
def experiment_result_dir(expName):
    check_exp_name_ok(expName)
    target_dir = experiment_base_result_dir(expName) + expName + '/'
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)
    return target_dir

# experiment_last_dir('0testDec17', '/noise_filtered_updates/')
# Returns (and creates if it didn't existed)
# '/srv/agarcia/beacon_mrai/ris_beacons/0testDic17/noise_filtered_updates/'
def experiment_last_dir(expName, result_string):
    base_dir = experiment_result_dir(expName)
    directory = base_dir + result_string + '/'
    if not os.path.isdir(directory):
        print('Creating directory {}').format(directory)
        os.mkdir(directory)
    return directory

# experiment_last_dir('0testDec17', 'rfd_updates/', 'rrc00')
# Returns (and creates if it didn't existed)
# '/srv/agarcia/beacon_mrai/ris_beacons/0testDic17/rrc00/rfd_updates/'
def per_collector_dir(expName, result_string, collector):
    result_dir = experiment_last_dir(expName, result_string)
    directory = result_dir + collector + '/'
    if not os.path.isdir(directory):
        print('Creating directory {}').format(directory)
        os.mkdir(directory)
    return directory


# filename_csv('0testDec17', 'rrc00', 'short_rfd')
# generates a filename of the type
#   .../short_rfd/rrc00.20171101.2.csv
# NOT to use with raw_bgpelems
def filename_csv_per_collector(expName, collector, last_dir):
    base_dir = experiment_result_dir(expName)
    starting_day = experiment_init_day(expName)
    number_days = experiment_number_days(expName)
    directory = base_dir + last_dir
    if not os.path.isdir(directory):
        print('Creating directory {}').format(directory)
        os.mkdir(directory)
    return directory + collector + '.' + starting_day + '.' + str(number_days) + '.csv'


# Writes README file in last_dir, with the contents of readme_text
# Intended to write module/function docstring in .py file generating the data
# into the directory in which the data is.
# e.g.: write_README('0testDec17', 'short_rfd', save_short_report.__doc__)
def write_README(expName, last_dir, readme_text):
    directory = experiment_result_dir(expName) + last_dir
    if not os.path.isdir(directory):
        print('Creating directory {}').format(directory)
        os.mkdir(directory)
    filename = directory + '/README'
    with open(filename, 'wb') as readme_file:
        readme_file.write(readme_text)


# Test (and create, if needed) per collector directories

# Returns (interval_number, interval_type) for a given update
# (34, 'A')
# (35, 'W')
def interval_number_and_type(expName, update):
    if 'ris_beacons' == experiments[expName]['beaconType']:
        return ris_interval_number_and_type(expName, update)
    else:
        # define for other types
        raise Exception('only defined for RIS')
    
# returns (interval_number, init_time of interval, interval_type 'A'/'W)
def ris_interval_number_and_type(expName, update):
    # could use prefix in the future (for non-ris beacons)
    elemtype, timestamp, monitor, peer_asn, fields = update

    # [(1509494400, 1509501600, 'A'), (1509501600, 1509508800, 'W'), 
    interval_list = experiment_beacon_adv_wd_times(expName)
    interval_number = 0
    for interval in interval_list:
        if timestamp >= interval[0] and timestamp < interval[1]:
            return (interval_number, interval[0], interval[2])
        interval_number += 1
    
    # not found in any interval is an error
    print('noise_filtered_updates2RFD_updates: ERROR, timestamp {} could not be found in interval, exiting').format(timestamp)
    exit(1)

def ris_interval_init_timestamp(expName, interval_number):
    interval_list = experiment_beacon_adv_wd_times(expName)
    return(interval_list[interval_number][0])


def interval_init_timestamp(expName, interval_number):
    if 'ris_beacons' == experiments[expName]['beaconType']:
        return ris_interval_init_timestamp(expName, interval_number)
    else:
        # define for other types
        raise Exception('only defined for RIS')    

# CLI
# --print_test 0testDec17
if __name__ == "__main__":
    parser = ArgumentParser()
    # ./experiment_manifest.py --print_test 0testDec17
    parser.add_argument("--print_test", help='--print_test TEST_NAME, prints info for considered test', default='')
    parser.add_argument("--collector_names", help='--collector_names TEST_NAME, returns collector names active for considered test (to be used by shell)')

    args= parser.parse_args()

    if args.print_test:
        _print_experiment_info(args.print_test)
    if args.collector_names:
        print(experiment_collectors(args.collector_names))
    else:
        print('Nothing requested... exiting')
