# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import subprocess

file_path = '/Users/nachogutierrez/Documents/traffic_analysis/data/updates.20171030.2335.gz'
bggdump_path = '/usr/local/bin/bgpdump'

update_lines  = subprocess.check_output ([ bggdump_path, '-m', file_path]).strip().split('\n')

