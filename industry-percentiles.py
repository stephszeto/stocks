
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
from os import path
import os.path
import gspread
import configs
import requests
import datetime
import csv
import numpy as np
import pandas as pd

print('Warming up ...')
begin_time = datetime.datetime.now()
today = datetime.date.today()

# here you have to enter your actual API key 
api_key = configs.api_key

# access google sheet
scope = ['https://spreadsheets.google.com/feeds']
creds = ServiceAccountCredentials.from_json_keyfile_name('google-sheets-secret.json', scope)
client = gspread.authorize(creds)

# open spreadsheet
spreadsheet_key = configs.company_key
sheet = client.open_by_key(spreadsheet_key)

# pull values from sheets
industry_tickers = sheet.worksheet("US by industry")
to_pull = sheet.worksheet("to pull")
# industries = industry_tickers.col_values(4)[1:]
# industries = list(set(industries))
industries = to_pull.col_values(2)[1:]
all_rows = industry_tickers.get_all_values()

# set up lists to hold data per metric
revPerShare = []
eps = []
ocfPerShare = []
fcfPerShare = []
cashPerShare = []
marketCap = []
ev = []
pe = []
ps = []
pocf = []
pfcf = []
debtToAssets = []
currentRatio = []
rdToRevenue = []
capexToRevenue = []
roic = []
roe = []

revGrowth = []
netIncomeGrowth = []
growthEPS = []
debtGrowth = []
ocfGrowth = []
fcfGrowth = []
rdGrowth = []
sgaGrowth = []


grossMargin = []
operatingMargin = []
netMargin = []

km_metrics = {"revenuePerShareTTM": revPerShare,
            "netIncomePerShareTTM": eps,
            "operatingCashFlowPerShareTTM": ocfPerShare,
            "freeCashFlowPerShareTTM": fcfPerShare,
            "cashPerShareTTM": cashPerShare,
            "marketCapTTM": marketCap,
            "enterpriseValueTTM": ev,
            "peRatioTTM": pe,
            "priceToSalesRatioTTM": ps,
            "pocfratioTTM": pocf,
            "pfcfRatioTTM": pfcf,
            "debtToAssetsTTM": debtToAssets,
            "currentRatioTTM": currentRatio,
            "researchAndDevelopementToRevenueTTM": rdToRevenue,
            "capexToRevenueTTM": capexToRevenue,
            "roicTTM": roic,
            "roeTTM": roe,
            }

growth_metrics = {"revenueGrowth": revGrowth,
            "netIncomeGrowth": netIncomeGrowth,
            "epsgrowth": growthEPS,
            "debtGrowth": debtGrowth,
            "operatingCashFlowGrowth": ocfGrowth,
            "freeCashFlowGrowth": fcfGrowth,
            "rdexpenseGrowth": rdGrowth,
            "sgaexpensesGrowth": sgaGrowth
            }

ratio_metrics = {"grossProfitMarginTTM": grossMargin,
            "operatingProfitMarginTTM": operatingMargin,
            "netProfitMarginTTM": netMargin,
            }

sources = [['https://financialmodelingprep.com/api/v3/key-metrics-ttm/', km_metrics],
            ['https://financialmodelingprep.com/api/v3/financial-growth/', growth_metrics],
            ['https://financialmodelingprep.com/api/v3/ratios-ttm/', ratio_metrics]
            ]

metrics = {**km_metrics, **growth_metrics, **ratio_metrics}

# create headers
headers, output = [], []
percentiles = [10, 25, 50, 75, 90]
metric_keys = metrics.keys()

for metric in metrics:
    if metric not in growth_metrics.keys():
        metric = metric[:len(metric) - 3]
    for percentile in percentiles:
        headers.append(metric + str(percentile))

headers.insert(0, 'Industry')
headers.insert(1, '# / firms')

percentiles_file = 'percentiles.csv'
if path.exists(percentiles_file):
    with open(percentiles_file, 'a') as file:
        headers_to_upload = headers 
        headers_to_upload.insert(0,"") # need to add extra space to account for extra col added with pandas
        csv_writer = csv.writer(file)
        csv_writer.writerow([])
        csv_writer.writerow(headers)
else:
    with open(percentiles_file, 'w') as file:
        headers_to_upload = headers 
        headers_to_upload.insert(0,"") # need to add extra space to account for extra col added with pandas
        csv_writer = csv.writer(file)
        csv_writer.writerow([])
        csv_writer.writerow(headers)

# get list of available symbols
url = 'https://financialmodelingprep.com/api/v3/stock/list?apikey=' + api_key
response = requests.get(url)
response = response.json()

# add the data from the single row returned
all_tickers = []
for item in response:
    all_tickers.append(item['symbol'])

# fetch data per industry
for industry in industries: 
    if industry == "":
        continue

    print('\n' + "Calculating metrics for "+ industry)
    # retrieve tickers per industry
    tickers = []
    for row in all_rows:
        if row[3] == industry and row[0] in all_tickers:
            tickers.append(row[0])

    # clear lists
    for key in metric_keys:
        metrics[key] = []

    # fetch data 
    for ticker in tickers:
        ticker = ticker.split(".")[0]
        print("Gathering data for "+ ticker)

        for source in sources:
            url = source[0] + ticker + '?limit=1&apikey=' + api_key
            keys = source[1].keys()

            # get response
            response = requests.get(url)
            response = response.json()

            # add the data from the single row returned
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

                for key in keys:
                    if item[key] == None:
                        print("No " + key + " data found for " + ticker)
                        continue
                    metrics[key].append(item[key])
    
    # calculate percentiles 
    row = [industry, len(metrics["revenuePerShareTTM"])]
    for key in metric_keys:
        for percentile in percentiles: 
            if len(metrics[key]) > 0:
                value = np.percentile(metrics[key], percentile)
            else:
                value = ""
            row.append(value)

    # create row 
    output += [row]

    # append to csv
    with open(percentiles_file, 'a') as file:
        row.insert(0,"")
        csv_writer = csv.writer(file)
        csv_writer.writerows([row])

# print('Headers: ' + str(headers) + '\n')
# print('Output: ' + str(output) + '\n')

# make dataframe from output 
table = pd.DataFrame(output, columns=headers)
print('Percentile table: ' + '\n' + str(table) + '\n')

# upload table
d2g.upload(table, spreadsheet_key, 'percentiles', credentials=creds, row_names=True)    

print('Execution time: ' + str(datetime.datetime.now() - begin_time))
print('Finito!')