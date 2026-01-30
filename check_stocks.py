import pandas as pd
import transform_stock_code as tsc
from collections import Counter
import os


if __name__ == "__main__":
    path = 'finance_data/'
    file_name_list = os.listdir(path)
    stock_code_df = pd.read_csv("hk_stock_list_short.csv", dtype={"股份代號": str}) # whole stock list
    whole_stock_list = stock_code_df['股份代號'].unique().tolist()
    type_stock = {'zyzb': [], 'zcfzb': [], 'lrb': [], 'xjllb': []}

    with open("fail_stocks.txt", "r") as f:
        fail_stock_lines = f.readlines()
    
    # Parse into dictionary
    fail_stock_reasons = {}
    for line in fail_stock_lines:
        if ':' in line:
            stock_code, reason = line.strip().split(':', 1)  # split on first ':' only
            fail_stock_reasons[stock_code.strip()] = reason.strip()

    for file_name in file_name_list:
        file_type = file_name.split('_')[0]
        df = pd.read_csv(os.path.join(path, file_name))
        df = tsc.transform_stock_code(df, '股票代码')
        df = df.drop_duplicates()
        stock_list = df['股票代码'].unique().tolist()
        type_stock[file_type].extend(stock_list)
        df.to_csv(os.path.join(path, file_name), index=False)
        print(f"Cleaning file: {file_name}")
    
    # Stocks that are successfully scraped in all 4 types
    all_values = [set(v) for v in type_stock.values()]
    common_stock = list(set.intersection(*all_values))
    
    # Failed Stocks
    failed_stocks = set(whole_stock_list) - set(common_stock)
    print("Failed stocks:", failed_stocks)

    re_scrape_stocks = []
    for fs in failed_stocks:
        if fs in fail_stock_reasons.keys() and fail_stock_reasons[fs] == 'Exceeded maximum retry attempts':
            re_scrape_stocks.append(fs)
        if fs not in fail_stock_reasons.keys():
            re_scrape_stocks.append(fs)
        else:
            print(f"{fs}: {fail_stock_reasons[fs]}")
            
    print("Stocks to re-scrape:", re_scrape_stocks)
