import argparse
import configs
import requests
import json
import csv
from lxml import html
from collections import OrderedDict
import statistics as stats

import warnings
warnings.filterwarnings('ignore')

def fetch_industries():
    print("Fetching industries data ... ")
    url = 'https://financialmodelingprep.com/api/v3/stock-screener?apikey=' + api_key
    response = requests.get(url).json()

    # pull industry per ticker
    us_industries, intl_industries, company_vals = {}, {}, {}
    for item in response:
        # break into industries 
        industry = item['industry']
        ticker = item['symbol']
        country = item['country']
        if industry and country == "US":
            add_industry(ticker, industry, us_industries)
        elif industry:
            add_industry(ticker, industry, intl_industries)

        # parse company stats
        price, shares = item['price'], 0
        if price > 0:
            shares = round(item['marketCap'] / price)
        if item['isActivelyTrading']:
            ticker = item['symbol']
            company_vals[ticker] = {'country': item['country'],
                                    'industry': item['industry'],
                                    'market_cap': item['marketCap'],
                                    'beta': item['beta'],
                                    'price': price,
                                    'shares': shares} 

    # parse number of tickers in each industry
    industries_list = [us_industries, intl_industries]
    for industries in industries_list:
        for industry in industries:
            industries[industry]['count'] = len(industries[industry]['tickers'])

    return us_industries, intl_industries, company_vals

def add_industry(ticker, industry, industries):
    if industry in industries.keys():
        # print("Adding " + ticker + ' to ' + industry)
        current_tickers = industries[industry]['tickers']
        current_tickers.append(ticker)
        industries[industry]['tickers'] = current_tickers
    else:
        # print("Adding " + industry)
        industries[industry] = {'tickers': [ticker]}

# print out other related industry tickers given a ticker      
def print_industry_tickers(ticker):
    industry = company_vals[ticker]['industry']
    country = company_vals[ticker]['country']
    if country == "US" and show_industry_tickers:
        print(industry + ' (US): ' + str(us_industries[industry]['tickers']))
    elif show_industry_tickers:
        print(industry + ' (Global): ' + str(intl_industries[industry]['tickers']))

# pull country ERP, default spreads, corporate tax rates
def fetch_country_data():
    country_data = {}
    with open("erps.csv", "r") as file:
        for line in csv.reader(file):
            country = line[1]
            data = line[2:] # includes ERP, default spread, country risk premium, corporate tax rate
            country_data[country] = data
    return country_data

def fetch_industry_stats(input, type):
    if type == "ticker":
        ticker = input
        industries, industry = get_industries(ticker)
        country = company_vals[ticker]['country']

    # pull all other tickers for the ticker's industry
    industry_tickers = industries[industry]['tickers']

    early_avg_growth, early_avg_margin = [], []
    middle_avg_growth, middle_avg_margin = [], []
    mature_avg_growth, mature_avg_margin = [], []
    total_avg_growth, total_avg_margin = [], []
    early_tickers, middle_tickers, mature_tickers = [], [], []
    company_stats = {}
    # pull rev growth, operating margin numbers over time 
    for ticker in industry_tickers:
        url = 'https://financialmodelingprep.com/api/v3/income-statement/' + ticker + '?limit=150&apikey=' + api_key
        response = requests.get(url).json()

        rev_growth_rates = []
        operating_margins = []
        for i in range(len(response)):
            item = response[i]
            revenue = item['revenue']
            operating_income = item['operatingIncome']
            if revenue != 0:
                operating_margin = round(operating_income / revenue, 4)
                operating_margins.append(operating_margin)

            if i != len(response) - 1:
                previous_item = response[i + 1]
                previous_rev = previous_item['revenue']
                if previous_rev == 0:
                    continue
                rev_growth_rate = round((revenue - previous_rev) / previous_rev, 4)
                rev_growth_rates.append(rev_growth_rate)
 
        rev_growth_rates.reverse()
        operating_margins.reverse()
        age = len(operating_margins)
        if len(rev_growth_rates) > 0:
            avg_growth = stats.mean(rev_growth_rates)
        if len(operating_margins) > 0:
            mid_life = round(age / 2)
            avg_mid_life_margin = stats.mean(operating_margins[mid_life:]) # want margin to be more heavily focused on later company stage

        # add to list of averages for future calculation
        # cap outliers when calculating total averages
        if avg_growth:
            if avg_growth > 5 or avg_growth < -5:
                avg_growth = 5 if avg_growth > 0 else -5
            total_avg_growth.append(avg_growth)

        if avg_mid_life_margin > 5:        
            avg_mid_life_margin = 5 if avg_mid_life_margin > 0 else -5
        total_avg_margin.append(avg_mid_life_margin)

        # track metrics by company age / stage
        if age < 6: 
            if avg_growth:
                early_avg_growth.append(avg_growth) 
            early_avg_margin.append(avg_mid_life_margin)
            early_tickers.append(ticker)
        elif age < 11:
            if avg_growth:
                middle_avg_growth.append(avg_growth) 
            middle_avg_margin.append(avg_mid_life_margin)
            middle_tickers.append(ticker)
        else:
            if avg_growth:
                mature_avg_growth.append(avg_growth) 
            mature_avg_margin.append(avg_mid_life_margin)
            mature_tickers.append(ticker)

        # save data 
        ticker_stats = {'rev_growth_rates': rev_growth_rates, 'operating_margins': operating_margins, 'age': age, 'avg_growth': avg_growth, 'avg_mid_life_margin': avg_mid_life_margin}
        company_vals[ticker].update(ticker_stats)
        company_stats[ticker] = ticker_stats

    # print out stats for further examination
    if show_industry_stats:
        company_stats = OrderedDict(sorted(company_stats.items(), key=lambda kv: kv[1]['age']))
        for ticker in company_stats.keys():
            value = company_stats[ticker]
            rev_growth_rates = value['rev_growth_rates']
            operating_margins = value['operating_margins']
            print(ticker + ' (' + str(value['age']) + ' years)')
            print("Average growth: " + convert(value['avg_growth']))
            print("Average mid-life margin: " + convert(value['avg_mid_life_margin']))
            print([convert(rate) for rate in rev_growth_rates])
            print([convert(margin) for margin in operating_margins])
            print('')

    avg_growth = stats.mean(total_avg_growth)
    avg_margin = stats.mean(total_avg_margin)

    early_avg_growth = stats.mean(early_avg_growth)
    early_avg_margin = stats.mean(early_avg_margin)

    middle_avg_growth = stats.mean(middle_avg_growth)
    middle_avg_margin = stats.mean(middle_avg_margin)

    mature_avg_growth = stats.mean(mature_avg_growth)
    mature_avg_margin = stats.mean(mature_avg_margin)

    small_dict = {'tickers': early_tickers, 'avg_growth': early_avg_growth, 'avg_margin': early_avg_margin, 'count': early_tickers.count}
    middle_dict = {'tickers': middle_tickers, 'avg_growth': middle_avg_growth, 'avg_margin': middle_avg_margin, 'count': middle_tickers.count}
    mature_dict = {'tickers': mature_tickers, 'avg_growth': mature_avg_growth, 'avg_margin': mature_avg_margin, 'count': mature_tickers.count}
    update_dict = {'avg_growth': avg_growth, 'avg_margin': avg_margin, 'small': small_dict, 'middle': middle_dict, 'mature': mature_dict}
    
    if country == "US":
        us_industries[industry].update(update_dict)
    else: 
        intl_industries[industry].update(update_dict)
    return avg_growth, avg_margin

def fetch_metrics(ticker):
    data = {'ticker': ticker}
    print("Fetching data for " + ticker)

    # pull and parse company details
    data.update(company_vals[ticker])

    # pull and parse income statement 
    url = 'https://financialmodelingprep.com/api/v3/income-statement/' + ticker + '?period=quarter&limit=8&apikey=' + api_key
    response = requests.get(url).json()

    if len(response) == 0:
        return []

    if show_metrics:
        print("TTM Period: {}".format(response[0]['date']))
        print("YoY TTM Period: {}".format(response[4]['date']) + '\n')

    income_statement_ttm, income_statement_last_ttm = response[:4], response[4:8]
    rev_ttm, op_income_ttm, interest_ttm, taxes_ttm, taxable_income_ttm = 0, 0, 0, 0, 0
    for item in income_statement_ttm:
        rev_ttm += item['revenue']
        op_income_ttm += item['operatingIncome']
        interest_ttm += item['interestExpense']
        taxes_ttm += item['incomeTaxExpense']
        taxable_income_ttm += item['incomeBeforeTax']

    rev_last_ttm = 0
    for item in income_statement_last_ttm:
        rev_last_ttm += item['revenue']

    rev_growth = (rev_ttm - rev_last_ttm) / rev_last_ttm if rev_last_ttm > 0 else 0
    effective_tax_rate = taxes_ttm / taxable_income_ttm  

    income_vals = {'rev_ttm': rev_ttm, 'rev_last_ttm': rev_last_ttm, 'rev_growth': rev_growth, 'effective_tax_rate': effective_tax_rate, 'op_income_ttm': op_income_ttm, 'interest_ttm': interest_ttm}
    data.update(income_vals)

    # pull and parse balance sheet
    url = 'https://financialmodelingprep.com/api/v3/balance-sheet-statement/' + ticker + '?period=quarter&limit=4&apikey=' + api_key
    response = requests.get(url).json()

    equity_ttm, debt_ttm, cash_ttm = 0, 0, 0
    for item in response[:4]:
        equity_ttm += item['totalStockholdersEquity']
        debt_ttm += item['totalDebt']
        cash_ttm += item['cashAndShortTermInvestments']

    bs_vals = {'cash_ttm': cash_ttm, 'equity_ttm': equity_ttm, 'debt_ttm': debt_ttm}
    data.update(bs_vals)

    # pull and parse cash flow statement 
    url = 'https://financialmodelingprep.com/api/v3/cash-flow-statement/' + ticker + '?period=quarter&limit=4&apikey=' + api_key
    response = requests.get(url).json()

    fcf_ttm = 0
    for item in response[:4]:
        fcf_ttm += item['freeCashFlow']

    cf_vals = {'fcf_ttm': fcf_ttm}
    data.update(cf_vals)

    # pull and parse earning estimates 
    url = "https://financialmodelingprep.com/api/v3/analyst-estimates/" + ticker + "?apikey=" + api_key
    response = requests.get(url).json()

    # if no estimates returned, default to existing growth rates / margins
    if len(response) == 0:
            print("Note: no estimates given for " + ticker + " - using existing revenue growth numbers.")
            avg_rev_growth_current_yr, low_rev_growth_current_yr, high_rev_growth_current_yr = rev_growth, rev_growth, rev_growth
            avg_rev_growth_next_yr, low_rev_growth_next_yr, high_rev_growth_next_yr = rev_growth, rev_growth, rev_growth
            current_op_margin = op_income_ttm / rev_ttm

            estimates_dict = {'rev_growth_estimates': {current_year: [low_rev_growth_current_yr, avg_rev_growth_current_yr, high_rev_growth_current_yr]},
                                'margin_estimates': {current_year: [current_op_margin, current_op_margin, current_op_margin]}}  
    else:
        # reverse response entries to go from old to new and pull out relevant estimates
        estimates = []
        response.reverse()
        for item in response:
            year = int(item['date'].split("-")[0])
            rev_growth_estimates = {}
            if year >= current_year:
                estimates.append(item)

        # if no relevant estimates returned, default to existing growth rates / margins
        if len(estimates) == 0:
            print("Note: no estimates given for " + ticker + " - using existing revenue growth numbers.")
            avg_rev_growth_current_yr, low_rev_growth_current_yr, high_rev_growth_current_yr = rev_growth, rev_growth, rev_growth
            avg_rev_growth_next_yr, low_rev_growth_next_yr, high_rev_growth_next_yr = rev_growth, rev_growth, rev_growth
            current_op_margin = op_income_ttm / rev_ttm

            estimates_dict = {'rev_growth_estimates': {current_year: [low_rev_growth_current_yr, avg_rev_growth_current_yr, high_rev_growth_current_yr]},
                                'margin_estimates': {current_year: [current_op_margin, current_op_margin, current_op_margin]}}            
        else: 
            # calculate growth rates
            avg_rev_key = 'estimatedRevenueAvg'
            low_rev_key = 'estimatedRevenueLow'
            high_rev_key = 'estimatedRevenueHigh'
            avg_ebit_key = 'estimatedEbitAvg'
            low_ebit_key = 'estimatedEbitLow'
            high_ebit_key = 'estimatedEbitHigh'
            avg_rev_growth_current_yr = (estimates[0][avg_rev_key] - rev_ttm) / rev_ttm
            low_rev_growth_current_yr = (estimates[0][low_rev_key] - rev_ttm) / rev_ttm
            high_rev_growth_current_yr = (estimates[0][high_rev_key] - rev_ttm) / rev_ttm
            avg_margin_current_yr = estimates[0][avg_ebit_key] / estimates[0][avg_rev_key]
            low_margin_current_yr = estimates[0][low_ebit_key] / estimates[0][low_rev_key]
            high_margin_current_yr = estimates[0][high_ebit_key] / estimates[0][high_rev_key]

            estimates_dict = {'rev_growth_estimates': {current_year: [low_rev_growth_current_yr, avg_rev_growth_current_yr, high_rev_growth_current_yr]},
                                'margin_estimates': {current_year: [low_margin_current_yr, avg_margin_current_yr, high_margin_current_yr]}}
            for i in range(1, len(estimates)):
                estimate = estimates[i]
                last_estimate = estimates[i - 1]
                avg_rev_growth = (estimate[avg_rev_key] - last_estimate[avg_rev_key]) / last_estimate[avg_rev_key]
                low_rev_growth = (estimate[low_rev_key] - last_estimate[low_rev_key]) / last_estimate[low_rev_key]        
                high_rev_growth = (estimate[high_rev_key] - last_estimate[high_rev_key]) / last_estimate[high_rev_key]  

                avg_margin = estimate[avg_ebit_key] / estimate[avg_rev_key]
                low_margin = estimate[low_ebit_key] / estimate[low_rev_key]
                high_margin = estimate[high_ebit_key] / estimate[high_rev_key]    

                year = int(estimate['date'].split("-")[0])
                estimates_dict['rev_growth_estimates'][year] = [low_rev_growth, avg_rev_growth, high_rev_growth]
                estimates_dict['margin_estimates'][year] = [low_margin, avg_margin, high_margin]
            
    data.update(estimates_dict)

    if show_metrics:
        print("// INCOME STATEMENT //")
        print_vals(income_vals, "number")
        print('\n' + "// BALANCE SHEET //")
        print_vals(bs_vals, "number")
        print('\n' + "// CASH FLOW //")
        print_vals(cf_vals, "number")
        print('\n' + "// GROWTH ESTIMATES //")
        print_vals(rev_growth_estimates)

    return data

# print values
def print_vals(dict, type="percent"):
    for key in dict.keys():
        value = dict[key]
        if isinstance(value, float) or isinstance(value, int):
            print(key + ": " + str(convert(value, type)))
        elif isinstance(value, list):
            print(key + ": " + str([convert(num, type) for num in value]))

# convert text into numbers
def parse(value):
    if value == "N/A":
        return value

    factor = 1
    if 'B' in value:
        factor = 1000000000
    elif 'M' in value:
        factor = 1000000
    return float(value.replace('M','').replace('B','')) * factor

def dcf(data, dcf_years=10):
    if len(data) == 0:
        return []

    print("Starting DCF calculation for " + data['ticker'])
    if debug:
        print('Data: ' + str(data))
        print('Company Vals: ' + str(company_vals[ticker]))
    
    # configure key settings
    perpetual_growth_rate = 0.02
    year_convergence = dcf_years
    future_riskfree_rate = 0.02
    type_estimate_used = "avg" # alternatively, "low" or "high"
    probability_failure = 0.1
    percent_proceeds_failure = 0.5

    # retrieve key inputs
    age = company_vals[ticker]['age']
    country = data['country']
    industries, industry = get_industries(ticker) 
    mid_point = round(dcf_years / 2)
    effective_tax_rate = data['effective_tax_rate']
    marginal_tax_rate = float(country_data[country][3])
    revenue = data['rev_ttm']
    if type_estimate_used == "avg":
        type_estimate_index = 1
    elif type_estimate_used == "low":
        type_estimate_index = 0
    elif type_estimate_used == "high":
        type_estimate_index = 2

    # calculate discount rate (cost of capital)
    # pull inputs for cost / equity 
    equity = data['market_cap']
    debt = data['debt_ttm']
    total_capital = equity + debt
    equity_weight = equity / total_capital
    debt_weight = debt / total_capital

    beta = data['beta']
    country_erp = float(country_data[country][0])

    # pull inputs for cost / debt
    market_cap = data['market_cap']
    interest = data['interest_ttm']
    if interest > 0:
        interest_coverage = data['op_income_ttm'] / data['interest_ttm']
    else:
        interest_coverage = 100000
    country_default_spread = float(country_data[country][1])

    cost_capital = calculate_cost_capital(equity_weight, debt_weight, beta, country_erp, interest_coverage, market_cap, country_default_spread)    
    perpetual_cost_capital = future_riskfree_rate + country_erp

    # populate operating margins
    current_margin = data['op_income_ttm'] / revenue

    # populate rev growth numbers, tax_rate numbers
    growth_rates = ['']
    op_margins = [current_margin]
    tax_rates = [effective_tax_rate, effective_tax_rate]
    costs_capital = ['', cost_capital]

    # either take average overall industry margin or average mature company margin 
    industry_growth_rate = industries[industry]['avg_growth']
    avg_industry_margin = industries[industry]['avg_margin']
    mature_avg_industry_margin = industries[industry]['mature']['avg_margin']
    industry_margin = max(avg_industry_margin, mature_avg_industry_margin)

    # populate first half of growth rates, margins
    rev_estimates = data['rev_growth_estimates']
    margin_estimates = data['margin_estimates']

    for i in range(len(rev_estimates)):
        year = current_year + i
        growth_rates.append(rev_estimates[year][type_estimate_index]) 
        op_margins.append(margin_estimates[year][type_estimate_index])

    # if known estimates > mid-point, fill in the gap with the latest numbers    
    if len(rev_estimates) < mid_point:   
        for i in range(len(rev_estimates), mid_point):
            growth_rates.append(growth_rates[-1])
            op_margins.append(op_margins[-1])

    # fill out second half of growth rate, margins rate 
    margin_estimates_list = []
    for year in margin_estimates.keys():
        margin_estimates_list.append(margin_estimates[year][1])

    if age <= 10 and (industry_margin > current_margin or industry_margin > stats.mean(margin_estimates_list)):
        target_margin = industry_margin
        reason = "industry margin (" + type_estimate_used + ")"
    else: 
        target_margin = company_vals[ticker]['avg_mid_life_margin']
        reason = "average margin (mid-life until now)"

    # provide context on how growth, margin rates chosen
    if show_dcf_calc:
        print('\n' + "Industry Growth Rate: " + convert(industry_growth_rate))
        print("Industry Margin Rate: " + convert(avg_industry_margin) + '\n')

        # track metrics by company age / stage
        if age < 6:
            print("Industry Growth Rate (Peer Group / Small): " + convert(industries[industry]['small']['avg_growth']))
            print("Industry Margin Rate (Peer Group / Small): " + convert(industries[industry]['small']['avg_margin']) + '\n')        
        elif age < 11:
            print("Industry Growth Rate (Peer Group / Middle): " + convert(industries[industry]['middle']['avg_growth']))
            print("Industry Margin Rate (Peer Group / Middle): " + convert(industries[industry]['middle']['avg_margin']) + '\n')   

        print("Industry Growth Rate (Mature): " + convert(industries[industry]['mature']['avg_growth']))
        print("Industry Margin Rate (Mature): " + convert(industries[industry]['mature']['avg_margin']) + '\n')  

        print("Target margin: " + convert(target_margin) + " (reason: " + reason + ")")

        if reason.split(" ")[0] == "average":
            print("Margins: " + str([convert(margin) for margin in (company_vals[ticker]['operating_margins'])]) + '\n')


    # setting up for next for loop
    remaining_period = dcf_years - mid_point
    rev_growth = growth_rates[-1]
    if len(rev_estimates) < mid_point:
        start_point = mid_point
    else:
        start_point = len(margin_estimates)

    for year in range(start_point, dcf_years):
        growth_rate = rev_growth - ((rev_growth - perpetual_growth_rate) / remaining_period) * (year - mid_point) 
        growth_rates.append(growth_rate)

        op_margin = target_margin - ((target_margin - current_margin) / year_convergence) * (year_convergence - year)
        op_margins.append(op_margin)

    # fill out first half of rates 
    for i in range(2, mid_point + 1):
        tax_rates.append(effective_tax_rate)
        costs_capital.append(cost_capital)

    # fill out second half of rates (not including terminal value)
    for year in range(mid_point + 1, dcf_years + 1):
        previous_year = year - 1
        tax_rate = tax_rates[previous_year] + ((marginal_tax_rate - effective_tax_rate) / remaining_period) 
        tax_rates.append(tax_rate)

        future_cost_capital = costs_capital[previous_year] - ((cost_capital - perpetual_cost_capital) / remaining_period)
        costs_capital.append(future_cost_capital)
    
    # add terminal value rates
    growth_rates.append(perpetual_growth_rate)
    tax_rates.append(marginal_tax_rate)
    op_margins.append(target_margin)
    costs_capital.append(perpetual_cost_capital)

    # project out revenues
    future_revs = [revenue]
    for year in range(1, len(growth_rates)):
        future_rev = future_revs[year - 1] * (1 + growth_rates[year])
        future_revs.append(future_rev)

    # project out operating incomes
    future_op_incomes = [data['op_income_ttm']]
    for year in range(1, len(op_margins)):
        future_op_income = op_margins[year] * future_revs[year]
        future_op_incomes.append(future_op_income)    

    # project out after-tax op incomes
    future_aftertax_op_incomes = []
    for year in range(len(future_op_incomes)):
        tax_rate = tax_rates[year]
        if tax_rate > 0:
            future_aftertax_op_income = future_op_incomes[year] * (1 - tax_rate)
        else:
            future_aftertax_op_income = future_op_incomes[year]
        future_aftertax_op_incomes.append(future_aftertax_op_income)

    # set sales / capital
    sales_capital = revenue / total_capital
    sales_capital_rates = [sales_capital] + [sales_capital for i in range(dcf_years + 1)] 

    # project out reinvestment capital
    reinvestment_yr1 = (future_revs[1] - future_revs[0]) * sales_capital_rates[0] if future_revs[1] > future_revs[0] else 0
    reinvestments = ['', reinvestment_yr1]
    for year in range(2, dcf_years + 1):
        reinvestment = (future_revs[year] - future_revs[year - 1]) * sales_capital_rates[year]
        reinvestments.append(reinvestment)
    reinvestments.append((perpetual_growth_rate / perpetual_cost_capital) * future_aftertax_op_incomes[-1])

    # project out free cash flows / firm
    future_fcffs = [data['fcf_ttm']]
    for year in range(1, len(future_aftertax_op_incomes)):
        fcff = future_aftertax_op_incomes[year] - reinvestments[year]
        future_fcffs.append(fcff)

    # calculate discount factors across every year 
    discount_factors = ['', 1 / (1 + costs_capital[1])]
    for year in range(2, len(costs_capital)):
        discount_factor = discount_factors[year - 1] * (1 / (1 + costs_capital[year]))
        discount_factors.append(discount_factor)

    # calculate present value of future FCFFs
    present_values = ['']
    for year in range(1, len(discount_factors) - 1):
        present_value = discount_factors[year] * future_fcffs[year]
        present_values.append(present_value)

    # set invested_capital
    invested_capitals = [total_capital]
    for year in range(1, len(reinvestments)):
        invested_capital = invested_capitals[year - 1] + reinvestments[year]
        invested_capitals.append(invested_capital)

    # print final interim values
    if show_dcf_calc:
        headers = ['', "Base"] + ["Year " + str(i) for i in range(1, dcf_years + 1)] + ["Terminal Value"]
        col_names = ["Rev Growth", "Revenue", "Op Margin", "Op Income / EBIT", "Tax Rate", "After-tax EBIT", "- Reinvestment", "FCFF", "Cost / Capital", "Discount Factors", "PV(FCFF)", "", "Sales / Capital", "Invested Capital"]
        col_vals = [growth_rates, future_revs, op_margins, future_op_incomes, tax_rates, future_aftertax_op_incomes, reinvestments, future_fcffs, costs_capital, discount_factors, present_values, [], sales_capital_rates, invested_capitals]
        for i in range(len(col_names)):
            col_vals[i].insert(0, col_names[i])
        types = ["percent", "number", "percent", "number", "percent", "number", "number", "number", "percent", "percent", "number", "", "percent", "number"]
        print_table(headers, col_vals, types)

    # calculating final terminal value and summing up together
    terminal_cf = future_fcffs[-1]
    terminal_value = terminal_cf * (perpetual_cost_capital - future_riskfree_rate)
    pv_terminal_value = terminal_value * discount_factors[-1]
    sum_pvs = round(sum(present_values[2:]), 2)
    total_pv = sum_pvs + pv_terminal_value

    # adjusting for probability of failure
    book_value = debt + data['equity_ttm']
    proceeds_failure = book_value * percent_proceeds_failure
    op_assets_value = probability_failure * proceeds_failure + (1 - probability_failure) * total_pv

    # take out debt, add cash
    equity_value = op_assets_value - debt + data['cash_ttm']

    if show_dcf_calc:
        print('\n' + "Terminal value = terminal cash flow * (perpetual cost / capital - future riskfree rate)")
        print("Terminal value = " + convert(terminal_cf, "number") + " * (" + convert(perpetual_cost_capital) + " - " + convert(future_riskfree_rate) + ")")
        print("Terminal value = " + convert(terminal_value, "number") + '\n')
        print("PV(terminal value) = terminal value * last discount factor")
        print("PV(terminal value) = " + convert(terminal_value, "number") + ' * ' + convert(discount_factors[-1]))
        print("PV(terminal value) = " + convert(pv_terminal_value, "number") + '\n')
        print("Total PV = PV(sum of cash flows) + PV(terminal value)")
        print("Total PV = " + convert(sum_pvs, "number") + " + " + convert(pv_terminal_value, "number"))
        print("Total PV = " + convert(total_pv, "number") + '\n')
        print("Operating assets value = P(failure) * proceeds / failure + P(success) * total PV")
        print("Operating assets value = " + convert(probability_failure) + ' * ' + convert(proceeds_failure, "number") + " + " + convert(1 - probability_failure) + " * " + convert(total_pv, "number"))
        print("Operating assets value = " + convert(op_assets_value, "number") + '\n')
        print("Equity value = operating assets value - debt + net cash")
        print("Equity value = " + convert(op_assets_value, "number") + " - " + convert(debt, "number") + " + " + convert(data['cash_ttm'], "number"))
        print("Equity value = " + convert(equity_value, "number"))

    print('\n' + ticker + " DCF valuation: {}".format(convert(equity_value, "number")))
    market_cap_difference = "N/A"
    if dcf != 0:
        market_cap_difference = (market_cap - equity_value) / equity_value
        print("Current market cap: {} ({}% over valuation)".format(convert(market_cap, "number"), round(market_cap_difference * 100, 2)))

    fair_price = round(equity_value / data['shares'], 2)
    current_price = data['price']
    print('\n' + ticker + " fair value price: {} ".format(fair_price))

    if fair_price != 0:
        price_difference = (current_price - fair_price) / fair_price
        print("Current price: {} ({}% over fair value)".format(current_price, round(price_difference * 100, 2)))

    return [market_cap, equity_value, current_price, fair_price, market_cap_difference]

# helper function to calculate cost / capital
def calculate_cost_capital(equity_weight, debt_weight, beta, country_erp, interest_coverage, market_cap, country_default_spread):
    # retrieve riskfree rate
    url = "https://finance.yahoo.com/quote/%5ETNX/"
    response = requests.get(url, verify=False)
    parser = html.fromstring(response.content)
    riskfree_rate = float(parser.xpath('//*[@id="quote-header-info"]/div[3]/div[1]/div/span[1]')[0].text) / 100

    # calculate cost / equity
    cost_equity = riskfree_rate + beta * country_erp # calculating ERP based off of country of incorporation
    if show_dcf_calc:
        print('\n' + "Equity %: " + convert(equity_weight))
        print("Cost / equity = riskfree rate + beta * country ERP")
        print("Cost / equity = " + convert(riskfree_rate) + " + " + str(beta) + " * " + convert(country_erp, "number"))
        print("Cost / equity = " + convert(cost_equity) + '\n')

    # calculate cost / debt
    default_spreads = [0.0069,0.0085,0.0107,0.0118,0.0133,0.0171,0.0231,0.0277,0.0405,0.0486,0.0594,0.0946,0.0997,0.1309,0.1744]
    if market_cap > 5000000000 and beta <= 1:
        interest_coverages = [8.50,6.5,5.5,4.25,3,2.5,2.25,2,1.75,1.5,1.25,0.8,0.65,0.2,-100000]
        
        for i in range(len(interest_coverages)):
            if interest_coverage > interest_coverages[i]:
                firm_default_spread = default_spreads[i]
                break
    else:
        interest_coverages = [12.5,9.5,7.5,6,4.5,4,3.5,3,2.5,2,1.5,1.25,0.8,0.5,-100000]

        for i in range(len(interest_coverages)):
            if interest_coverage > interest_coverages[i]:
                firm_default_spread = default_spreads[i]
                break

    synthetic_cost_debt = riskfree_rate + country_default_spread + firm_default_spread
    # market_debt = data['interest_ttm'] * (1 - ((1 + synthetic_cost_debt) ** -4)) / synthetic_cost_debt + data['debt_ttm'] / ((1 + synthetic_cost_debt) ** 4)

    effective_tax_rate = data['effective_tax_rate']
    cost_debt = synthetic_cost_debt * (1 - effective_tax_rate)
    if show_dcf_calc:
        print("Debt %: " + convert(debt_weight))
        print("Cost / debt = synthetic cost / debt * (1 - effective tax rate)")
        print("Cost / debt = " + convert(synthetic_cost_debt) + ' * (1 - ' + convert(effective_tax_rate) + ')')
        print("Cost / debt = " + convert(cost_debt) + '\n')

    # combine it all together to get cost / capital / discount rate
    cost_capital = equity_weight * cost_equity + debt_weight * cost_debt
    if show_dcf_calc:
        print("Cost / capital: " + convert(cost_capital))

    return cost_capital

# convert floats into prettier percentages & large values into more readable terms 
def convert(float, type="percent"):
    if isinstance(float, str): 
        return float 
    if type == "percent":
        if isinstance(float, str): 
            return float 
        if float >= 0: 
            float = round(float * 100)
            return str(float) + '%'
        else:
            return "-" + convert(-float)
    elif type == "number":
        if float >= 0: 
            if float > 1000000000:
                float /= 1000000000
                float = round(float, 2)
                return str(float) + 'B'
            elif float > 1000000:
                float /= 1000000
                float = round(float, 2)
                return str(float) + 'M'
            elif float > 1000:
                float /= 1000
                float = round(float, 2)
                return str(float) + 'K'         
            else:
                float = round(float, 2)
                return str(float)
        else:
            return "-" + convert(-float, "number")

# print table
def print_table(headers, rows, types=[]):
    # str_l = max(len(str(t)) for t in rows) 
    str_l = 15
    print(" ".join(['{:>{length}s}'.format(t, length = str_l) for t in headers]))

    i = 0
    if len(types) == 0:
        types = ['' for i in range(len(rows))]
    for row in rows:
        if types[i] == "number":
            print(" ".join(['{:>{length}s}'.format(convert(x, "number"), length = str_l) for x in row]))
        elif types[i] == "percent":
            print(" ".join(['{:>{length}s}'.format(convert(x), length = str_l) for x in row]))
        else:
            print(" ".join(['{:>{length}s}'.format(str(x), length = str_l) for x in row]))
        i += 1

def graham(data):
    return
    # if data['eps'] > 0:
    #     expected_value = data['eps'] * (8.5 + 2 * (data['ge']))
    #     ge_priced_in = (data['mp'] / data['eps'] - 8.5) / 2

    #     print("Expected value based on growth rate: {}".format(expected_value))
    #     print("Growth rate priced in for next 7-10 years: {}\n".format(ge_priced_in))
    # else:
    #     print("Not applicable since EPS is negative.")

def get_industries(ticker):
    industry = company_vals[ticker]['industry']
    country = company_vals[ticker]['country']
    if country == "US":
        return us_industries, industry
    else:
        return intl_industries, industry    

if __name__ == "__main__":
    global current_year
    current_year = 2021

    argparser = argparse.ArgumentParser()
    argparser.add_argument('-t', '--ticker', dest = 'ticker', default = "", help='Ticker to calculate DCF for (e.g. AMZN)')
    argparser.add_argument('-l', '--list', nargs="*", type = str, default = [], dest = 'ticker_list', help='Space-separated list of tickers to calculate DCFs for (e.g. AMZN GOOG MSFT)')
    argparser.add_argument('-i', '--industry', action='store_true', dest = 'calculate_industry_dcfs', default = False, help='Calculate DCFs for all companies in industry')
    argparser.add_argument('-y', '--dcf_years', dest = 'dcf_years', default = 10, help='Customize the number of years to run DCF for')
    argparser.add_argument('-d', '--debug', action='store_true', dest = 'debug_setting', default = False, help='Verbose mode for debugging')

    args = argparser.parse_args()

    # no inputs given
    if len(args.ticker_list) == 0 and args.ticker == "":
        print("Please input either ticker or list of tickers to run. Aborting!")
        quit()

    tickers = []
    if args.ticker != "":
        tickers = [args.ticker.upper()]
    if len(args.ticker_list) > 0:
        tickers = tickers + [ticker.upper() for ticker in args.ticker_list]
        args.calculate_industry_dcfs = False # if already pulling for a list of tickers, do not pull for industries as well

    # settings
    global debug, show_metrics, show_dcf_calc, show_industry_tickers, show_industry_stats
    debug = True if args.debug_setting else False
    show_metrics = True if args.debug_setting else False
    calculate_industry_dcfs = True if args.calculate_industry_dcfs else False
    show_industry_stats = False
    show_industry_tickers = True
    show_dcf_calc = True
    dcf_years = int(args.dcf_years)

    # collect industry, country data 
    global api_key
    global us_industries, intl_industries, company_vals, country_data
    api_key = configs.api_key
    us_industries, intl_industries, company_vals = fetch_industries()    
    country_data = fetch_country_data()

    dcfs = []
    headers = ["Ticker", "Age", "Market Cap", "DCF", "Price", "Fair Value", "% Difference"]
    for ticker in tickers:
        print("Processing " + ticker)
        print_industry_tickers(ticker)

        # calculate DCFs
        growth, margin = fetch_industry_stats(ticker, "ticker") 
        industries, industry = get_industries(ticker)
        industry_tickers = industries[industry]['tickers']

        # if calculating multiple DCFs for multiple specified tickers
        if not args.calculate_industry_dcfs:
            data = fetch_metrics(ticker)
            dcf_values = dcf(data, dcf_years)

            if len(tickers) > 1:
                dcf_values = [ticker, company_vals[ticker]['age'], convert(dcf_values[0], "number"), convert(dcf_values[1], "number"), convert(dcf_values[2], "number"), convert(dcf_values[3], "number"), convert(dcf_values[4], "percent")]
                dcfs.append(dcf_values)

        # if calculating industry DCFs for one ticker
        if args.calculate_industry_dcfs:
            valid_tickers = 0
            for ticker in industry_tickers:
                data = fetch_metrics(ticker)
                dcf_values = dcf(data, dcf_years)
                if len(dcf_values) > 0:
                    dcf_values = [ticker, company_vals[ticker]['age'], convert(dcf_values[0], "number"), convert(dcf_values[1], "number"), convert(dcf_values[2], "number"), convert(dcf_values[3], "number"), convert(dcf_values[4], "percent")]
                    dcfs.append(dcf_values)
                    valid_tickers += 1

            print(industry + " summary (" + str(valid_tickers) + " of " + str(len(industry_tickers)) + " tickers):")
            print_table(headers, dcfs)

    # print out final summary for ticker list input after all data collected
    if not args.calculate_industry_dcfs and len(tickers) > 1:
        print_table(headers, dcfs)