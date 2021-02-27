from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import requests
import pandas as pd
import datetime
import configs

print('Warming up ...')

# here you have to enter your actual API key 
api_key = configs.api_key

# access google sheet
scope = ['https://spreadsheets.google.com/feeds']
creds = ServiceAccountCredentials.from_json_keyfile_name('google-sheets-secret.json', scope)
client = gspread.authorize(creds)

# open spreadsheet
spreadsheet_key = configs.overview_key
sheet = client.open_by_key(spreadsheet_key)

# pull values from sheets
to_pull = sheet.worksheet("to pull")
tickers = to_pull.col_values(1)[1:]
industry_tickers = to_pull.col_values(4)[1:]
exclude_list = to_pull.col_values(10)[1:]
etf_tickers = to_pull.col_values(3)[1:]

# combine them together / dedupe / remove blanks
tickers += industry_tickers
tickers = list(set(tickers)) 
no_blank_ticker = []
for ticker in tickers:
    if len(ticker) > 0 and ticker not in exclude_list:
        no_blank_ticker.append(ticker)
tickers = no_blank_ticker


# define how many years we want to retrieve
num_years = 1

# variable to store things
income_cols, income_output = [], []
cf_cols, cf_output = [], []
ratio_ttm_cols, ratio_ttm_output = [], []
km_ttm_cols, km_ttm_output = [], []

quote_cols, quote_output = [], []
list_cols, list_output = [], []

sector_cols, sector_output = [], []
comm_cols, comm_output = [], []
index_cols, index_output = [], []

# declare what data to pull
# single sources are for sources that can only take one ticker at a time
single_sources, bulk_sources = [], []
if len(tickers) > 0: 
    single_sources = [
                ['income-statement/', 'with-limit', income_cols, income_output, 'income'],
                ['cash-flow-statement/','with-limit', cf_cols, cf_output, 'cf'],
                ['ratios-ttm/','ticker-only', ratio_ttm_cols, ratio_ttm_output, 'ratio-ttm'],
                ['key-metrics-ttm/','ticker-only', km_ttm_cols, km_ttm_output, 'km-ttm']
            ]

# bulk sources are sources that can take multiple tickers or only need to be called once
bulk_sources = [
            ['quote/','multiple-tickers', quote_cols, quote_output, 'quotes'],
            ['stock-list','',list_cols, list_output, 'tickers'],
            ['quotes/commodity','no-input', comm_cols, comm_output, 'commodities'],
            ['sectors-performance','no-input', sector_cols, sector_output, 'sector-perf']
            # ['quotes/index', 'no-input', index_cols, index_output, 'indices'],
        ]

sources = bulk_sources + single_sources

print(tickers)
# fetch data
for source in single_sources: 
    for ticker in tickers:
        print("Fetching "+ source[4] + ' for ' + ticker)

        type, param_type = source[4], source[1]
        # configure urls 
        url = 'https://financialmodelingprep.com/api/v3/'
        if param_type == 'ticker-only':
            url += source[0] + ticker + '?apikey=' + api_key
        if param_type == 'with-limit':
            url += source[0] + ticker + '?limit=' + str(num_years) + '&apikey=' + api_key
        
        # get response
        response = requests.get(url)
        response = response.json()

        # add each row in response
        for item in response:
            # don't add if there's an error
            if item == 'Error Message' or isinstance(item, str):
                if len(response) == 1:
                    print(str(response) + '\n')
                    quit()
                else:
                    print(item)
                    print("Error for one row - skipping" + '\n')
                break

            # add symbol data for TTM sources
            if type == 'km-ttm' or type == 'ratio-ttm':
                symbol = {'symbol': ticker}
                symbol.update(item)
                item = symbol

            # add columns    
            keys = list(item.keys())
            if len(source[2]) == 0:
                source[2] += keys

            source[3] += [list(item.values())]

print(tickers)

# fetch data from API
for source in bulk_sources:
    type, param_type = source[4], source[1]    
    print('Fetching ' + type)

    # configure urls 
    url = 'https://financialmodelingprep.com/api/v3/'
    if type == 'tickers':
            url += '/stock-screener?marketCapMoreThan=10000000&exchange=nasdaq,nyse,euronext&isActivelyTraded=true&apikey=' + api_key        
    if param_type == 'no-input':
        url += source[0] + '?apikey=' + api_key
    if param_type == 'multiple-tickers':
        if type == 'quotes':
            extended_tickers = tickers
            extended_tickers.extend(etf_tickers)
        url += source[0] + ','.join(extended_tickers) + '?apikey=' + api_key 

    # get response
    response = requests.get(url)
    response = response.json()

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

        # add columns    
        keys = list(item.keys())
        if len(source[2]) == 0:
            source[2] += keys

        # update names for commodities
        if type == 'commodities':
            new_name = item['name']
            name = item['name'].lower()

            commodities = {'cotton': 'Cotton', 
                            'palladium': 'Palladium',
                            'silver': 'Silver', 
                            'brent': 'Oil Brent', 
                            'sugar': 'Sugar', 
                            'soybeans': 'Soybeans', 
                            'lean hogs': 'Lean Hogs', 
                            'feeder cattle': 'Feeder Cattle', 
                            'natural gas': 'Natural Gas', 
                            'crude oil': 'Oil WTI', 
                            'rough rice': 'Rough Rice', 
                            'heating oil': 'Heating Oil', 
                            'platinum': 'Platinum', 
                            'coffee': 'Coffee', 
                            'wheat': 'Wheat', 
                            'cocoa': 'Cocoa', 
                            'live cattle': 'Live Cattle', 
                            'copper': 'Copper', 
                            'gold': 'Gold', 
                            'lumber': 'Lumber', 
                            'soybean oil': 'Soybean Oil', 
                            'corn': 'Corn', 
                            'propane': 'Propane', 
                            'gasoline': 'Gasoline', 
                            }

            for commodity in list(commodities.keys()): 
                if commodity in name and 'brent' not in name:
                    new_name = commodities[commodity]
                    break
                if commodity in name and '100 oz' not in name:
                    new_name = commodities[commodity]
                    break
                if commodity in name:
                    new_name = commodities[commodity]
                    break
            item['name'] = new_name

        # update % for sector perf
        if type == 'sector-perf':
            value = item['changesPercentage'][:-1]
            item['changesPercentage'] = round(float(value),1)

        source[3] += [list(item.values())]

# print('Cols: ' + str(quote_cols) + '\n')
# print('Output: ' + str(quote_output) + '\n')

# make dataframe from output
for source in sources:
    table = pd.DataFrame(source[3], columns=source[2])

    # drop unwanted columns
    to_drop = []
    type = source[4]
    if type == 'tickers':
        to_drop = [5,6,7,8,9,10,11,12,13]
    if type == 'commodities':
        to_drop = [4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21] 
    if type == 'quotes':
        to_drop = [5,6,12,13,14,15,16,19,21]
    if type == 'income':
        to_drop = [3,4,10,11,12,13,14,15,16,17,19,20,21,22,23,24,25,27,28,29,30,31,32,33]
    if type == 'cf':
        to_drop = [4,6,7,8,9,10,11,12,13,14,15,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,34,36,37]
    if type == 'ratio-ttm':
        to_drop = [1,2,3,4,5,6,7,8,9,10,12,16,18,22,23,24,29,32,33,34,35,36,37,38,39,40,41,46,47,48,39,40,41,46,47,48,49,50,51,52,53,54,55]
    if len(to_drop) > 0:
        table.drop(table.columns[to_drop], axis=1, inplace=True)

    # upload table
    print(type + ' table: ' + '\n' + str(table) + '\n')
    d2g.upload(table, spreadsheet_key, type, credentials=creds, row_names=True)    

now = datetime.datetime.now()
to_pull.update('H1', "Last updated: " + '{d.month}/{d.day} {d.hour}:{d.minute:02}'.format(d=now))
to_pull.update('I1', "Need to be converted")

print('Finito!')