"""
    Author: Glen Chin

    CSV Extractor (Github Repository)
    Tool to retrieve COVID-19 cases from JHU CSSE COVID 19 Data
    https://github.com/CSSEGISandData/COVID-19

    Notes:
        - Currently supports retrieving COVID-19 data from Singapore and Pima County, Arizona, US only
        - Extracted data is written into a new csv file 'cases.csv' within the same directory
        - Two options: Generate new csv or update previously created csv
"""




from datetime import datetime
import requests
import csv
import sys
import os

# 14-column header (latest as of 5 Feb, 2022)
header = ['Date', 'FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active', 'Combined_Key', 'Incidence_Rate', 'Case-Fatality_Ratio']

# Index of the Country_Region column in each of 6-8-12-14 column formats
country_index = {
    6: 1,
    8: 1,
    12: 3,
    14: 3
}

target = {'US': {'Arizona': ['Pima']}, 'Singapore': {}}

def format_datestr(datestr):
    """
    Format a date string to match mm-dd-YYYY and fill in leading zeroes
    """
    if '/' in datestr:
        datestr = datestr.split('/')
    elif '-' in datestr:
        datestr = datestr.split('-')
    datestr[0] = datestr[0].zfill(2)
    datestr[1] = datestr[1].zfill(2)

    return '-'.join(datestr)

def update_format(row, cols, datestr):
    """
    Handle two early formats that vary significantly.
    This method simply swaps the columns around to match the current format

    Note: There are a total of 4 different formats gathered from all csv files. 
    Each format has 6, 8, 12, 14 columns respectively, where 6 and 8 is out of order, while 12 differs by an addition of two columns at the end, and 14 being the one we want to match.
    """
    if cols >= 12:
        new_row = [datestr] + row
    else:
        new_row = [None] * 15

        # Arrange the columns to match latest 14-columns format
        new_row[0] = datestr
        new_row[2] = row[0]
        new_row[4] = row[1]
        new_row[5] = row[2]
        new_row[8] = row[3]
        new_row[9] = row[4]
        new_row[10] = row[5]
        if cols == 8:
            new_row[6] = row[6]
            new_row[7] = row[7]

    return new_row

def append_contents(writer, filenames, file_url):
    """
    Sends a request for the files in the Git repository, retrieve, and write the information to csv 
    Parameters:
        writer: the file writer object
        filenames: the list of file names to search and write into the csv
        file_url: URL to Github repository
    """
    # For each file
    for fname in filenames:
        print(fname)
        furl = file_url + fname

        response = requests.get(furl, timeout=5)
        response_decoded = [line.decode('utf-8') for line in response.iter_lines()]
        contents = list(csv.reader(response_decoded))

        # Check columns are at a maximum of 14 columns
        col_names = contents[0]
        if (len(col_names) > 14):
            print('There are more than 14 columns and program needs to be updated. Exiting...')
            sys.exit(-1)

        # FUTURE: To add support for older data, where province_state column included cityname and state abbreviation. This should only be relevant to data before 03-22-2022

        for row in contents[1:]:
            # These two lines skip "additional" empty rows that appear when iterating through certain csv files. Does not affect data.
            if len(row) == 0 or row == None:
                continue

            isTarget = False
            index = country_index[len(row)]
            country = row[index]
            if country in target.keys():
                if len(target[country]) == 0:
                    # For countries with no Province_state
                    isTarget = True
                else:
                    province_state = row[index-1]
                    if province_state in target[country].keys():
                        if country == "US":
                            # County name
                            county =  row[index-2]
                            if county in target[country][province_state]:
                                isTarget = True
                        else:
                            isTarget = True

            if isTarget:
                row = update_format(row, len(col_names), fname.split('.')[0])
                writer.writerow(row)

def main():
    url = "https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports"
    file_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/"
    target_filename = "cases.csv"
    filenames = []

    # User prompt selection
    prompt = "Choose method by enter the corresponding number:\n1) Generate new CSV file\n2) Append existing CSV file\n"
    choice = input(prompt)
    while choice not in ('1', '2'):
        print("Invalid option. Please enter again:")
        choice = input(prompt)

    try:
        response = requests.get(url, timeout=10)
    except requests.exceptions.Timeout:
        # Timeout
        print('Connection timeout')
        sys.exit(-1)
    except requests.exceptions.TooManyRedirects:
        # Notify user of invalid URL
        print('Bad URL. Check URL and retry')
    except requests.exceptions.RequestException as e:
        # Error
        print('An error occured')
        raise SystemExit(e)

    directory = response.json()

    for file in directory:
        if file['type'] == 'file' and file['name'].split('.')[1] == 'csv':
            filenames.append(file['name'])

    filenames.sort(key = lambda fname: datetime.strptime(fname.split('.')[:1][0], '%m-%d-%Y'))

    if choice == '1':
        with open(os.path.join(sys.path[0], target_filename), 'w', newline='') as f:
            # Create the csv writer and write column headers
            writer = csv.writer(f)
            writer.writerow(header)
            append_contents(writer, filenames, file_url)

    elif choice == '2':
        # Identify last recorded date and append newest
        with open(os.path.join(sys.path[0], target_filename), "r") as csvfile:
            csvreader = csv.reader(csvfile)
            for line in csvreader:
                pass

        last_date_str = format_datestr(line[0]) + '.csv'
        index = filenames.index(last_date_str)
        filenames = filenames[index+1:]
        
        if len(filenames) == 0:
            print("Already up to date")
        else:
            with open(os.path.join(sys.path[0], target_filename), 'a', newline='') as f:
                # Create the csv writer
                writer = csv.writer(f)
                append_contents(writer, filenames, file_url)
    else:
        print('Something went wrong')
        sys.exit(-1)
    
    print("Completed")

if __name__ == "__main__":
    main()