#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
This script manages input/output files. Basically it is a refactorization of repetitive code in several files

"""

import pandas as pd


def save_file(df, ext, output_file_path):
    if ext == '.xlsx':
        writer = pd.ExcelWriter(output_file_path, engine='xlsxwriter')
        df.to_excel(writer, sheet_name='Sheet1', index=False)
        writer.save()
        print(' Excel File saved in ' + output_file_path)

    if ext == '.csv':
        df.to_csv(output_file_path)
        print(' CSV File saved in ' + output_file_path)
