#!/usr/bin/env bash

# Script that launches every python code needed

# To debug this file, 
# bash -x ./experiment.sh

# strict mode, http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail

# ./experiment.sh 1mar18_2w 0testApr18
TEST_VECTOR_HELP="
run as\n
   nohup ./experiment.sh NAME_EXP NAME_ANALYSIS \"TEST_VECTOR\" \&\n
   nohup ./experiment.sh 0testDec17 0testAbr18 \"5 6\" \&\n
\n
 special case: test only one collector: use TEST string\n
 ./experiment.sh TEST          # runs all tests\n
 ./experiment.sh TEST \"5 6\"    # runs only test 5 and 6\n
 TEST_VECTOR defines which code to execute\n
\n
1   - Download ANCHORS and BEACONS\n
2   - Compute ANCHOR and STATE activity timestamps\n
3   - Generate noise filtered UPDATES\n
4   - Check beacon/monitor time synchronization\n
5   - Generate RFD reports and UPDATES\n
6   - Generate Adv and Wd timestamps for MRAI determination\n
"

# rrc18 has few data, allows fast tests
SINGLE_COLLECTOR_FOR_TESTS='rrc00 rrc04 rrc07 rrc10'

# Check the 'execute_command_for_each_collector' line in each code 
# block to see if it is active or not (steps not needed are disabled)

# Update as more tests are included
ALL_TESTS_VECTOR="1 2 3 4 5 6 7"

if [ $# -eq 0 ]; then
    echo -e './experiment.sh exp_name test_vector  \n    missing exp_name'
    echo ''
    echo -e $TEST_VECTOR_HELP
    exit 1
elif  [ "$1" == "TEST" ]; 
then
    # put here collector to use
    COLLECTOR_NAMES=$SINGLE_COLLECTOR_FOR_TESTS
    # EXP_NAME='0testDec17'
    EXP_NAME='experiment_2'
    ANAL_NAME='experiment_2'
    # test everything if number of arguments where 1 (experiment name)
    
    
    if [ $# -eq 2 ]; then
        TEST_VECTOR=$2
    elif [ $# -eq 1 ]; then
        TEST_VECTOR=$ALL_TESTS_VECTOR
    else
        exit 1
    fi
    echo "---------------"
    echo "Running TEST mode, all tests for collector $COLLECTOR_NAMES"
    echo "---------------"
else
    EXP_NAME=$1
    if [ $# -eq 2 ]; then
        TEST_VECTOR=$ALL_TESTS_VECTOR
    elif [ $# -gt 2 ]; then
        TEST_VECTOR=$3
    else
        echo 'Parameters not recognized'
        echo ''
        echo $TEST_VECTOR_HELP
        exit 1
    fi
    ANAL_NAME=$2
    
    COLLECTOR_NAMES=$( ./experiment_manifest.py --collector_names $EXP_NAME )
    # COLLECTOR_NAMES="route-views.eqix route-views.isc route-views.jinx route-views.linx route-views.nwax route-views.perth route-views.saopaulo route-views.sfmix route-views.sg route-views.soxrs route-views.sydney route-views.telxatl route-views.wide route-views2 route-views3 route-views4 rrc00 rrc01 rrc03 rrc04 rrc05 rrc06 rrc07 rrc10 rrc11 rrc12 rrc13 rrc14 rrc15 rrc16 rrc18 rrc19 rrc20"
    #COLLECTOR_NAMES="rrc21"
fi

echo "Executing exp_name: $EXP_NAME, analysis: $ANAL_NAME, test vector: $TEST_VECTOR"
LOG_DIR="/srv/agarcia/igutierrez/logs/"

# Be nice, do not execute more than K processes at the same time
# max 20        - K=20 processes
function max () {
   # Check running jobs only (not Done), show pids, count number of pids
   while [ `jobs -rp| wc -w` -gt $1 ]
   do
      sleep 5
   done
}

# C1=" ./experiment_manifest.py --collector_names $EXP_NAME,$collector,anchors "
# Note that it does not require any particular format for the commands to be called
function execute_command_for_each_collector () {
    for collector in $COLLECTOR_NAMES
    do
        # The following expands the variables here, so $collector has the appropriate value
        COMMAND=" $(eval echo "$1" ) "
        # $2 is the log file
        max 15; $COMMAND >> $2 &
        echo "EXECUTING $COMMAND"
    done
} 

# LOG_FILE: used to dump the output of the execution of the command
echo ""
echo "Execute commands and write logs to $LOG_DIR"
echo ""

# load updates
C1_LOG_FILE="${LOG_DIR}$EXP_NAME.load_raw_data.log"
# C1 is expanded INSIDE execute_command... function, so $collector is properly expanded
C1=' "./load_raw_data.py --load $EXP_NAME,$collector " ' 
# LOG_file need to be passed independently
# Execute only if "1" is in TEST_VECTOR
if [[ $TEST_VECTOR == *"1"* ]]; then
    execute_command_for_each_collector "$C1" $C1_LOG_FILE
fi 

# wait all previous commands to finish
# (first job is ./experiment.sh)
max 0

# compute anchor and state activity timestamps 
C2_LOG_FILE="${LOG_DIR}$EXP_NAME.sort_updates_for_cleaning.log"
C2=' " ./sort_updates_for_cleaning.py --load $EXP_NAME,$collector " '
if [[ $TEST_VECTOR == *"2"* ]]; then
    execute_command_for_each_collector "$C2" $C2_LOG_FILE
fi 


# wait all previous commands to finish
max 0

# compute noise_filtered_updates
C3_LOG_FILE="${LOG_DIR}$EXP_NAME.clean_data.log"
C3=' " ./clean_data.py --load $EXP_NAME,$collector" '
if [[ $TEST_VECTOR == *"3"* ]]; then
    execute_command_for_each_collector "$C3" $C3_LOG_FILE
fi

# wait all previous commands to finish
max 0

# compute noise_filtered_updates
C4_LOG_FILE="${LOG_DIR}$EXP_NAME.concatenate_RIB_data.log"
C4=' " ./concatenate_RIB_data.py --load $EXP_NAME,$collector" '
if [[ $TEST_VECTOR == *"4"* ]]; then
    execute_command_for_each_collector "$C4" $C4_LOG_FILE
fi

# wait all previous commands to finish
max 0

# compute noise_filtered_updates
C5_LOG_FILE="${LOG_DIR}$EXP_NAME.split_data_for_analysis.log"
C5=' " ./split_data_for_analysis.py --load $EXP_NAME,$collector" '
if [[ $TEST_VECTOR == *"5"* ]]; then
    execute_command_for_each_collector "$C5" $C5_LOG_FILE
fi

# wait all previous commands to finish
max 0

# compute noise_filtered_updates
C6_LOG_FILE="${LOG_DIR}$EXP_NAME.more_specifics_analysis.log"
C6=' " ./more_specifics_analysis.py --load $EXP_NAME,$collector" '
if [[ $TEST_VECTOR == *"6"* ]]; then
    execute_command_for_each_collector "$C6" $C6_LOG_FILE
fi

max 0

# compute noise_filtered_updates
C7_LOG_FILE="${LOG_DIR}$EXP_NAME.combine_collector_data.log"
C7=' " ./combine_collector_data.py --load $EXP_NAME,$collector" '
if [[ $TEST_VECTOR == *"7"* ]]; then
    execute_command_for_each_collector "$C7" $C7_LOG_FILE
fi

# Include per_collector_analysis2html_report (still executed in zompopo)
# Generates .md, executes markdown perl to convert to html
