from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta  
import datetime as dt
import pandas as pd
import yfinance as yf
import requests
import lxml
from functools import reduce

tickers = ['AAPL', 'AMZN', 'BLK', 'T', 'TSLA'] # <-- Initial Tickers List. It will be available globally for all functions.


####################################################
# 1. DEFINE PYTHON FUNCTIONS
####################################################

def fetch_prices_function(**kwargs): # <-- Remember to include "**kwargs" in all the defined functions 
    print('1 Fetching stock prices and remove duplicates...')
    stocks_prices = []
    for i in range(0, len(tickers)):
        prices = yf.download(tickers[i], period = 'max').iloc[: , :5].dropna(axis=0, how='any')
        prices = prices.loc[~prices.index.duplicated(keep='last')]
        prices = prices.reset_index()
        prices.insert(loc = 1, column = 'Stock', value = tickers[i])
        stocks_prices.append(prices)
    return stocks_prices  # <-- This list is the output of the fetch_prices_function and the input for the functions below
    print('Completed \n\n')
        
 
def stocks_plot_function(**kwargs): 
    print('2 Pulling stocks_prices to concatenate sub-lists to create a combined dataset + write to CSV file...')
    ti = kwargs['ti']
    stocks_prices = ti.xcom_pull(task_ids='fetch_prices_task') # <-- xcom_pull is used to pull the stocks_prices list generated above
    stock_plots_data = pd.concat(stocks_prices, ignore_index = True)
    stock_plots_data.to_csv('/Users/antonellobenedetto/Documents/Data_Sets/Medium/stocks_plots_data.csv', index = False)
    
    print('DF Shape: ', stock_plots_data.shape)
    print(stock_plots_data.head(5))
    print('Completed \n\n')


def stocks_table_function(**kwargs):
    print('3 Creating aggregated dataframe with stock stats for last available date + write to CSV file...')
    ti = kwargs['ti']
    stocks_prices = ti.xcom_pull(task_ids='fetch_prices_task') # <-- xcom_pull is used to pull the stocks_prices list generated above
    stocks_adj_close = []
    for i in range(0, len(stocks_prices)):
        adj_price= stocks_prices[i][['Date','Adj Close']]
        adj_price.set_index('Date', inplace = True)
        adj_price.columns = [tickers[i]]
        stocks_adj_close.append(adj_price)

    stocks_adj_close = reduce(lambda left,right: pd.merge(left, right, left_index = True, right_index = True ,how='outer'), stocks_adj_close)
    stocks_adj_close.sort_index(ascending = False, inplace = True)
    stocks_adj_close.index = pd.to_datetime(stocks_adj_close.index).date

    stocks_adj_close_f = stocks_adj_close.iloc[0] # <- creates a copy of the full df including last row only
    stocks_adj_close_f = stocks_adj_close_f.reset_index() # <- removing the index transforms the pd.Series into pd.DataFrame
    stocks_adj_close_f.insert(loc = 1, column = 'Date', value = stocks_adj_close_f.columns[1])
    stocks_adj_close_f.columns = ['Symbol', 'Date' , 'Adj. Price']
    stocks_adj_close_f.set_index('Symbol', inplace = True)

    #######################################

    def get_key_stats(tgt_website):
        df_list = pd.read_html(tgt_website)
        result_df = df_list[0]
        for df in df_list[1:]:
             result_df = result_df.append(df)
        return result_df.set_index(0).T

    stats = pd.DataFrame()
    statistics = []

    for i in range(0, len(tickers)):
        values =(get_key_stats('https://sg.finance.yahoo.com/quote/'+ str(tickers[i]) +'/key-statistics?p='+ str(tickers[i])))
        statistics.append(values)

    stats = stats.append(statistics)
    stats.reset_index(drop=True, inplace= True)
    stats.insert(loc=0, column='Symbol', value=pd.Series(tickers)) 
    stats.set_index(['Symbol'], inplace = True)

    stats = stats[['Market cap (intra-day) 5', 'Enterprise value 3', 'Trailing P/E',
                    'Forward P/E 1', 'PEG Ratio (5 yr expected) 1', 'Price/sales (ttm)',
                    'Price/book (mrq)', 'Enterprise value/revenue 3',
                    'Enterprise value/EBITDA 6', 'Beta (5Y monthly)', '52-week change 3',
                    'S&P500 52-week change 3', '52-week high 3', '52-week low 3',
                    '50-day moving average 3', '200-day moving average 3',
                    'Avg vol (3-month) 3', 'Avg vol (10-day) 3', 'Shares outstanding 5',
                    'Float', '% held by insiders 1', '% held by institutions 1',
                    'Forward annual dividend rate 4', 'Forward annual dividend yield 4',
                    'Trailing annual dividend rate 3', 'Trailing annual dividend yield 3',
                    '5-year average dividend yield 4', 'Payout ratio 4', 'Dividend date 3',
                    'Ex-dividend date 4', 'Last split factor 2', 'Last split date 3',
                    'Fiscal year ends', 'Most-recent quarter (mrq)', 'Profit margin',
                    'Operating margin (ttm)', 'Return on assets (ttm)',
                    'Return on equity (ttm)', 'Revenue (ttm)', 'Revenue per share (ttm)',
                    'Quarterly revenue growth (yoy)', 'Gross profit (ttm)', 'EBITDA',
                    'Net income avi to common (ttm)', 'Diluted EPS (ttm)',
                    'Quarterly earnings growth (yoy)', 'Total cash (mrq)',
                    'Total cash per share (mrq)', 'Total debt (mrq)',
                    'Total debt/equity (mrq)', 'Current ratio (mrq)',
                    'Book value per share (mrq)', 'Operating cash flow (ttm)',
                    'Levered free cash flow (ttm)']]

    stats.columns = ['Mkt_Cap', 'Enterpr_Value', 'P/E',
                      'P/E_(Forward)', 'PEG_Ratio', 'P/S',
                      'P/B', 'Enterpr_Value/Revenue','Enterpr_Value/EBITDA', 
                      'Beta_(5Y M)', '52W Change','S&P500_52W_Change', '52W_High', '52W_Low',
                      '50D Mov_Avg', '200D Mov_Avg.',
                      'Avg_Vol_(3M)', 'Avg_Vol_(10D)', 'Outst_Shares',
                      'Float', 'Insiders_Stake_pct', 'Institutions_Stake_pct',
                      'Dividend Rate_(F Annual)', 'Dividend Yield_(F Annual)',
                      'Dividend Rate_(T Annual)', 'Dividend_Yield_(T Annual)',
                      'Dividend Yield_(Avg 5Y)', 'Payout_Ratio', 'Dividend_Date',
                      'Ex-dividend_Date', 'Last_Split_Factor', 'Last_Split_Date',
                      'Fiscal_Year_Ends', 'MRQ', 'ProfMargin',
                      'Operating_Margin', 'ROA', 'ROE', 'Revenue12M', 'Revenue_p/s_(12M)',
                      'Quarterly_Revenue_Growth_(YoY)', 'Gross_Profit(L12M)', 
                      'EBITDA','Net Income 12M', 'Diluted EPS_(L12M)',
                      'Quarterly_Earnings_Growth_(YoY)', 'Total_Cash_(MRQ)',
                      'Total_Cash_Per_Share_(MRQ)', 'Total_Debt_(MRQ)',
                      'Total_Debt/Equity_(MRQ)', 'Current Ratio',
                      'Book Value p/s_(MRQ)', 'Ops CF 12M',
                      'Levered_Free_Cash_Flow_(L12M)']

    stats_c = stats.copy()
    stats_c = stats_c[['Mkt_Cap','P/B' ,'P/E', 'PEG_Ratio', 'ROA', 'ROE', 
                           'Revenue12M', 'Net Income 12M', 'ProfMargin', 'Ops CF 12M', 'Current Ratio']]

    stocks_table_data = pd.merge(stocks_adj_close_f, stats_c, left_index=True, right_index=True)
    
    print('DF Shape: ', stocks_table_data.shape)
    print(stocks_table_data.head(5)) 
    stocks_table_data.to_csv('/Users/antonellobenedetto/Documents/Data_Sets/Medium/stocks_table_data.csv', index = False)
    print('Completed')
    
                                                         
############################################
#2. DEFINE AIRFLOW DAG (SETTINGS + SCHEDULE)
############################################

default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'email': ['user@gmail.com'],
     'email_on_failure': False,
     'email_on_retry': False,
     'retries': 1
    }

dag = DAG( 'stocks_analysis_ETL_7AM',
            default_args=default_args,
            description='Collect Stock Prices For Analysis',
            catchup=False, 
            start_date= datetime(2020, 6, 23), 
            schedule_interval= '* 7 * * *'  
          )  

##########################################
#3. DEFINE AIRFLOW OPERATORS
##########################################

fetch_prices_task = PythonOperator(task_id = 'fetch_prices_task', 
                                   python_callable = fetch_prices_function, 
                                   provide_context = True,
                                   dag= dag )


stocks_plot_task= PythonOperator(task_id = 'stocks_plot_task', 
                                 python_callable = stocks_plot_function,
                                 provide_context = True,
                                 dag= dag)

stocks_table_task = PythonOperator(task_id = 'stocks_table_task', 
                                  python_callable = stocks_table_function,
                                  provide_context = True,
                                  dag= dag)      

##########################################
#4. DEFINE OPERATORS HIERARCHY
##########################################

fetch_prices_task  >> stocks_plot_task >> stocks_table_task

