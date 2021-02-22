from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import requests
import pandas as pd

print('Warming up ...' + '\n')

# here you have to enter your actual API key from SimFin
# here you have to enter your actual API key 
api_key = "PASTE YOUR API KEY HERE"

# access google sheet
scope = ['https://spreadsheets.google.com/feeds']
creds = ServiceAccountCredentials.from_json_keyfile_name('ADD THE FILE THAT HAS YOUR JSON SECRETS HERE', scope)
client = gspread.authorize(creds)

# open spreadsheet
spreadsheet_key = 'YOUR SPREADSHEET KEY GOES HERE'
sheet = client.open_by_key(spreadsheet_key)

universe_sheet = sheet.worksheet("S&P500")
# list of tickers we want to get data for
tickers = universe_sheet.col_values(1)
tickers = ["FB"]

# define the periods that we want to retrieve
periods = ["fy"]
year_start = 2019
year_end = 2020

# request urls
url = 'https://simfin.com/api/v2/companies/statements'

# variable to store the names of the columns
income_cols, bs_cols, cf_cols, derived_cols = [], [], [], []
# variable to store our data
income_output, bs_output, cf_output, derived_output = [], [], [], []

statements = ["pl", "bs", "cf", "derived"]

# if you don't have a SimFin+ subscription, you can only request data for single companies and one period at a time (with SimFin+, you can request multiple tickers and periods at once)
for ticker in tickers:
    print("Working on " + ticker)
    # loop through years:
    for year in range(year_start, year_end + 1):
        # loop through periods
        for period in periods:
            # loop through each statement type
            for statement in statements: 

                params = {"statement": statement, "ticker": ticker, "period": period, "fyear": year, "api-key": api_key}
                # make the request
                request = requests.get(url, params)

                # convert response to json and take 0th index as we only requested one ticker (if more than one ticker is requested, the data for the nth ticker will be at the nth position in the result returned from the API)
                data = request.json()[0]

                # make sure that data was found
                if data['found'] and len(data['data']) > 0:
                    if statement == "pl":
                        # add the column descriptions once only
                        if len(income_cols) == 0:
                            income_cols = data['columns']
                        # add the data
                        income_output += data['data']
                    elif statement == "bs":
                        # add the column descriptions once only
                        if len(bs_cols) == 0:
                            bs_cols = data['columns']
                        # add the data
                        bs_output += data['data']
                    elif statement == "cf":
                        # add the column descriptions once only
                        if len(cf_cols) == 0:
                            cf_cols = data['columns']
                        # add the data
                        cf_output += data['data']
                    elif statement == "derived":
                        # add the column descriptions once only
                        if len(derived_cols) == 0:
                            derived_cols = data['columns']
                        # add the data
                        derived_output += data['data']

# make dataframe from output
income_table = pd.DataFrame(income_output, columns=income_cols)
print('Income table: ' + str(income_table) + '\n')

bs_table = pd.DataFrame(bs_output, columns=bs_cols)
print('Balance Sheet table: ' + str(bs_table) + '\n')

cf_table = pd.DataFrame(cf_output, columns=cf_cols)
print('Cash Flow table: ' + str(cf_table) + '\n')

derived_table = pd.DataFrame(derived_output, columns=derived_cols)
print('Derived table: ' + str(derived_table) + '\n')

for statement in statements: 
    worksheet = sheet.worksheet(statement)
    if worksheet:
        sheet.del_worksheet(statement)

# upload all tables    
d2g.upload(income_table, spreadsheet_key, 'pl', credentials=creds, row_names=True)
d2g.upload(bs_table, spreadsheet_key, 'bs', credentials=creds, row_names=True)
d2g.upload(cf_table, spreadsheet_key, 'cf', credentials=creds, row_names=True)
d2g.upload(derived_table, spreadsheet_key, 'derived', credentials=creds, row_names=True)

print('Finito!')