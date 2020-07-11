#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 15 13:52:15 2020

@author: pyahia
"""


import os

path = r"/Users/pyahia/git/chewy-edi-bi-repository/EDI_Repository/data_ingestion/graph"
for root, dirs, files in os.walk(path):
    for file in files:
        with open(root+os.sep + file, "r") as f:
            content = f.read()
        if 'printStatements="true"' in content:
            content = content.replace('printStatements="true"','printStatements="false"')
            with open(root+os.sep + file, "w") as f:
                f.write(content)