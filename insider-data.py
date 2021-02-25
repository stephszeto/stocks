#!/usr/bin/env python3
from bs4 import BeautifulSoup
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import requests
import gspread

print("Warming up ...")

# retrieve data
urls = ["http://openinsider.com/screener?s=&o=&pl=&ph=&ll=&lh=&fd=365&fdr=&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&vl=25&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=5000&page=1",
"http://openinsider.com/screener?s=&o=&pl=&ph=&ll=&lh=&fd=365&fdr=&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&vl=25&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=5000&page=2"]

first = True 
data = []

for url in urls:
	html = requests.get(url).text

	# parse data
	soup = BeautifulSoup(html,"lxml")

	# retrieve table data
	results_table = soup.find("table", attrs={"class": "tinytable"})
	headers = results_table.thead.find_all("th")
	rows = results_table.tbody.find_all("tr")

	# Get all the headings of Lists
	if first: 
		headings = []
		for header in headers:
			th = header.text.strip()
			headings.append(th)
	
	for row in rows:
		# print('Row: ' + str(row) + '\n')
		cols = row.find_all('td')
		cols = [ele.text.strip() for ele in cols]
		row_data = []
		for ele in cols: 
			if len(ele) > 0:
				if ele[0] == '+' and ele[1] == "$":
					ele = ele[2:].split(",")
					ele = float(ele[0] + ele[1])
			row_data.append(ele)
		data.append(row_data)
	first = False

table = pd.DataFrame(data, columns=headings)
table = table.drop(table.columns[[0, 1, 8, 9, 10, 11, 13, 14, 15, 16]], axis=1)

print('Table: ' + str(table))

scope = ['https://spreadsheets.google.com/feeds']
creds = ServiceAccountCredentials.from_json_keyfile_name('google-sheets-secret.json', scope)
client = gspread.authorize(creds)

# upload to overview table
overview_key = '1tSXlKAzvQmISZKmJ0jHZFKm-nuK5pJUmoI1rpAoVGus'
sheet = client.open_by_key(overview_key)
d2g.upload(table, overview_key, 'raw-insider', credentials=creds, row_names=True)

to_pull = sheet.worksheet("to pull")
to_pull.update_cell(1, 6, "Need to be converted")
print("Uploaded to Overview sheet")

# upload to company table
company_key = '1M4iu3A_DzOfJZgsoFEQ-1KlvCApajLYx6LT0gdP7tC8'
sheet = client.open_by_key(company_key)
d2g.upload(table, company_key, 'raw-insider', credentials=creds, row_names=True)

company_sheet = sheet.worksheet("ANNUAL")
company_sheet.update_cell(3, 2, "Need to be converted")
print("Uploaded to Company sheet")

print('Finito!')