# Import Python packages
import pandas as pd
import re
import os
import glob
import numpy as np
import json
import csv
from sql_queries import *


def main():
    """
   This script serves as the ETL Pipeline which pre-process the Source CSV files and creates a consolidated CSV file for loading Apache Cassandra tables.

    Source/Target:
        - Source(s): Event files dataset(of CSV format)
        - Target(s): Consolidated CSV file named as event_datafile_new.csv
    """

    ####################################################################################
    # Creating list of filepaths to process original event csv data files
    ####################################################################################

    # event data directory
    filepath = os.getcwd() + '/event_data'

    # create list of all csv files in event_data/
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.csv'))
        for f in files:
            all_files.append(os.path.abspath(f))


    ######################################################################################################
    ## Processing the files to create the data file csv that will be used for Apache Casssandra tables
    ######################################################################################################
    all_data = []

    for file in all_files:
        # reading csv file
        with open(file, 'r', encoding='utf8', newline='') as csvfile:
            # create csv object
            csvreader = csv.reader(csvfile)
            # skip csv header
            next(csvreader)

            # append each line to all_data[]
            for line in csvreader:
                all_data.append(line)

    # creating new csv file with the relevant columns
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open('event_datafile_new.csv', 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', 'level', 'location', 'sessionId', 'song', 'userId'])
        for row in all_data:
            if row[0] == '':
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

    # check the number of rows in your csv file
    with open('event_datafile_new.csv', 'r', encoding='utf8') as f:
        print("Number of records in consolidated csv file = " + str(sum(1 for line in f)))



    print('')
if __name__ == "__main__":
    main()