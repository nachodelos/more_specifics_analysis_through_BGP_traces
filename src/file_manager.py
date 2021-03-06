#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
This script manages input/output files. Basically it is a refactorization of repetitive code in several files

"""

import pandas as pd
import os.path


def read_file(ext, input_file_path):
    if ext == '.xlsx':
        return pd.read_excel(input_file_path)
    elif ext == '.csv':
        return pd.read_csv(input_file_path, sep=',')


def save_file(df, ext, output_file_path):
    if ext == '.xlsx':
        writer = pd.ExcelWriter(output_file_path, engine='xlsxwriter')
        df.to_excel(writer, sheet_name='Sheet1', index=False)
        writer.save()
        print(' Excel File saved in ' + output_file_path)

    elif ext == '.csv':
        df.to_csv(output_file_path)
        print(' CSV File saved in ' + output_file_path)


def overwrite_file(output_file_path):
    
    if os.path.isfile(output_file_path):
        print "Output file already exists. Skipping this stage!"
        write_flag = False
    else:
        write_flag = True

    return write_flag
