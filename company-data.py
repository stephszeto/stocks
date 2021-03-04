from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import requests
import pandas as pd
import datetime
import time
import configs

print('Warming up ...')

# here you have to enter your actual API key 
api_key = configs.api_key

# access google sheet
scope = ['https://spreadsheets.google.com/feeds']
creds = ServiceAccountCredentials.from_json_keyfile_name('google-sheets-secret.json', scope)
client = gspread.authorize(creds)

# open spreadsheet
spreadsheet_key = configs.company_key
sheet = client.open_by_key(spreadsheet_key)
to_pull = sheet.worksheet("to pull")
company_sheet = sheet.worksheet("ANNUAL")

# # pull values from sheets
tickers = []
stocks = to_pull.col_values(1)[1:]
for stock in stocks: 
    if stock != '' and stock not in tickers:
        tickers.append(stock)

# define how many years, periods we want to retrieve
num_years = 20
num_periods = 8

# variable to store things
income_cols, income_output = [], []
bs_cols, bs_output = [], []
cf_cols, cf_output = [], []
ratio_cols, ratio_output = [], []
km_cols, km_output = [], []
share_cols, share_output = [], []

quote_cols, quote_output = [], []
prof_cols, prof_output = [], []
ratio_ttm_cols, ratio_ttm_output = [], []
km_ttm_cols, km_ttm_output = [], []

# declare what data to pull
# single sources are for sources that can only take one ticker at a time
single_sources = [
            ['income-statement/', 'with-limit', income_cols, income_output, 'income'],
            ['balance-sheet-statement/', 'with-limit', bs_cols, bs_output, 'bs'],
            ['cash-flow-statement/','with-limit', cf_cols, cf_output, 'cf'],
            ['ratios/','with-limit', ratio_cols, ratio_output, 'ratio'],
            ['key-metrics/','with-limit', km_cols, km_output, 'km'],
            ['ratios-ttm/','ticker-only', ratio_ttm_cols, ratio_ttm_output, 'ratio-ttm'],
            ['key-metrics-ttm/','ticker-only', km_ttm_cols, km_ttm_output, 'km-ttm'],
            ['financial-statement-full-as-reported/','ticker-only', share_cols, share_output, 'shares']
        ]

# bulk sources are sources that can take multiple tickers or only need to be called once
bulk_sources = [
            ['quote/','multiple-tickers', quote_cols, quote_output, 'quotes'],
            ['profile/','multiple-tickers', prof_cols, prof_output, 'profiles']
        ]

period_sources = ['income', 'bs', 'cf', 'ratio', 'km']

sources = single_sources + bulk_sources

# fetch data from API
for source in sources:
    for ticker in tickers:
        type, param_type = source[4], source[1] 
        print("Fetching "+ type + ' for ' + ticker)
           
        # configure urls 
        url = 'https://financialmodelingprep.com/api/v3/'
        if param_type == 'ticker-only':
            url += source[0] + ticker + '?apikey=' + api_key
        elif param_type == 'with-limit':
            url += source[0] + ticker + '?limit=' + str(num_years) + '&apikey=' + api_key 
        elif param_type == 'multiple-tickers':
            url += source[0] + ','.join(tickers) + '?apikey=' + api_key

        # get response
        response = requests.get(url)
        response = response.json()

        if type in period_sources:
            url = 'https://financialmodelingprep.com/api/v3/' + source[0] + ticker + '?period=quarter&limit=' + str(num_periods) + '&apikey=' + api_key 
            period_response = requests.get(url)
            period_response = period_response.json()

            new_resp, new_period_resp = [], []
            if type == 'ratio' or type == 'km':
                for item in response:
                    period = {'period': 'FY'}
                    period.update(item)
                    new_resp.append(period)

                for item in period_response:
                    month = item['date'].split("-")
                    month = month[1]
                    if month == "01" or month == "02" or month == "03":
                        quarter = 'Q1'
                    elif month == "04" or month == "05" or month == "06":
                        quarter = 'Q2'
                    elif month == "07" or month == "08" or month == "09":
                        quarter = 'Q3'
                    else:
                        quarter = 'Q4'
                    period = {'period': quarter}
                    period.update(item)
                    new_period_resp.append(period)
                response = new_resp + new_period_resp
            else:
                response += period_response

        if response == []:
            print("No data returned.")

        # add each row in response
        for item in response:
            # don't add if there's an error
            if item == 'Error Message':
                if len(response) == 1:
                    print(str(response) + '\n')
                    quit()
                else:
                    print("Error for one row - skipping" + '\n')
                break   

            # add symbol data for TTM sources
            if type == 'km-ttm' or type == 'ratio-ttm':
                symbol = {'symbol': ticker}
                symbol.update(item)
                item = symbol

            # discard most data for shares response
            keys = list(item.keys())
            if type == 'shares':
                to_keep = ["date", "symbol", "period", "commonstocksharesoutstanding"]
                updated_item = {}
                for key in to_keep:
                    if key in keys:
                        updated_item[key] = item[key]
                item = updated_item
                keys = list(item.keys())

            # add headers    
            if len(source[2]) == 0:
                source[2] += keys

            # convert values into floats if possible
            for key in keys:
                try: 
                    item[key] = float(item[key])
                except:    
                    # do nothing
                    pass

            # add rows
            source[3] += [list(item.values())]

        # only go through loop once if not ticker-specific  
        if source not in single_sources:
            break

# print('Cols: ' + str(quote_cols) + '\n')
# print('Output: ' + str(income_output) + '\n')

# make dataframe from output
for source in sources:
    table = pd.DataFrame(source[3], columns=source[2])

    # drop unwanted columns
    to_drop = []
    type = source[4]
    if type == 'quotes':
        to_drop = [3,4,5,6,10,11,12,13,14,15,16,17,18,19,21]
    if type == 'profiles':
        to_drop = [1,2,3,4,5,6,7,8,10,11,12,13,14,16,18,20,21,22,23,24,25,26,27,28,29,30,31,32,33];
    if type == 'km-ttm':
        to_drop = [8,9,17,18,19,20,21,26,28,29,30,31,32,33,34,35,36,37,38,39,42,44,45,47,48,49,50,51,52,55,56]
    if type == 'ratio-ttm':
        to_drop = [1,2,3,4,5,6,7,8,9,10,11,12,22,23,24,27,28,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,55,56,57]
    if type == 'income':
        to_drop = [4,9,19,21,22,23,24,25,27,29,30,31,32,33]
    if type == 'bs':
        to_drop = [4,8,16,30,31,32,33,38,39,41,45,46]
    if type == 'cf':
        to_drop = [4,36,37]
    if type == 'km':
        to_drop = [11, 20, 21, 22, 23, 28, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 56, 57, 58]
    if type == 'ratio':
        to_drop = [3, 4, 5, 6, 7, 8, 9, 10, 13, 19, 20, 21, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 53, 54, 55, 56]
    if len(to_drop) > 0:
        table.drop(table.columns[to_drop], axis=1, inplace=True)

    # upload table
    print(type + ' table: ' + '\n' + str(table) + '\n')
    d2g.upload(table, spreadsheet_key, type, credentials=creds, row_names=True) 
    time.sleep(60)

now = datetime.datetime.now()
company_sheet.update('B2', "Last updated: " + '{d.month}/{d.day} {d.hour}:{d.minute:02}'.format(d=now))
company_sheet.update('B3', "Need to be converted")

print('Finito!')