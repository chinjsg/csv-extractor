"""
    Author: Glen Chin

    CSV Extractor (Github Repository)
    Tool to retrieve COVID-19 cases from JHU CSSE COVID 19 Data
    https://github.com/CSSEGISandData/COVID-19

    Notes:
        - Currently supports retrieving COVID-19 data from Singapore only
        - Extracted data is written into new csv file 'covid19_singapore.csv'
        - Please backup file and/or ensure file is not used in other projects as this file will be overwritten
"""

from datetime import datetime
import requests
import csv
import sys
import os

def create_dict(columns, is_new):
    """
    Creates a dictionary to store indexes of columns of interest in the csv files
    Parameters:
        columns: a list of column headers (first row of csv file)
        is_new: a boolean indicating the format of csv file (old format has less columns/information)

    Returns:
        The column index dictionary for reference
    """
    index_dict = {}
    if is_new:
        index_dict['Country_Region'] = columns.index('Country_Region')
        index_dict['Active'] = columns.index('Active')
    else:
        index_dict['Country_Region'] = columns.index('Country/Region')

    # Common columns for both format types    
    index_dict['Confirmed'] = columns.index('Confirmed')
    index_dict['Deaths'] = columns.index('Deaths')
    index_dict['Recovered'] = columns.index('Recovered')

    return index_dict

def process(row, indexes, datestr):
    """
    Creates a dictionary to store indexes of columns of interest in the csv files
    Parameters:
        row: the row currently being read from the csv file
        indexes: the column index dictionary for accessing the columns of interest
        datestr: a string representing the date for this row data

    Returns:
        A formatted row as a list to be written in a csv file
        Contains the columns: Date, Country_Region, Confirmed, Deaths, Recovered, Active
    """
    country = row[indexes['Country_Region']]
    confirmed = row[indexes['Confirmed']]
    deaths = row[indexes['Deaths']]
    recovered = row[indexes['Recovered']]

    active = ''
    if 'Active' in indexes.keys():
        active = row[indexes['Active']]

    return [datestr, country, confirmed, deaths, recovered, active]


def main():
    url = "https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports"
    file_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/"
    filenames = []

    response = requests.get(url, timeout=5)
    directory = response.json()

    for file in directory:
        if file['type'] == 'file' and file['name'].split('.')[1] == 'csv':
            filenames.append(file['name'])

    filenames.sort(key = lambda fname: datetime.strptime(fname.split('.')[:1][0], '%m-%d-%Y'))

    with open(os.path.join(sys.path[0], 'covid19_singapore_copy.csv'), 'w', newline='') as f:
        # Create the csv writer and write column headers
        writer = csv.writer(f)
        writer.writerow(['Date', 'Country_Region','Confirmed', 'Deaths', 'Recovered', 'Active'])

        # For each file
        for fname in filenames:
            print(fname)
            furl = file_url + fname
            response = requests.get(furl, timeout=5)
            response_decoded = [line.decode('utf-8') for line in response.iter_lines()]
            contents = list(csv.reader(response_decoded))
            
            # Get column names and determine old/new format of file
            column_names = contents[0]
            format_new = False
            if len(column_names) > 8:
                format_new = True

            column_index = create_dict(column_names, format_new)

            # Write row to the csv file
            for row in contents:
                if row[column_index['Country_Region']] == 'Singapore':
                    record = process(row, column_index, fname.split('.')[0])
                    writer.writerow(record)

if __name__ == "__main__":
    main()