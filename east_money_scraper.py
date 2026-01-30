from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import pandas as pd
from io import StringIO
import transform_data_for_scraper as tdfs
import time
import random

def make_driver_without_proxy():
    options = Options()
    # options.add_argument("--headless=new")  # headless mode
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36")
    options.add_argument("--lang=zh-CN")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver


def make_driver_with_proxy(proxy):
    options = Options()
    options.add_argument(f'--proxy-server=http://{proxy}')
    options.add_argument("--headless=new")  # headless mode
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver


def open_url(driver, stock_code):
    url = f"https://emweb.securities.eastmoney.com/PC_HKF10/pages/home/index.html?code={stock_code}&type=web&color=w#/NewFinancialAnalysis"
    driver.get(url)


def get_finance_table(driver, wait, content_name="content_zyzb"):
    try:
        # Wait for the table element to exist in the DOM
        container = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f"div.content.{content_name}")))
        # Optionally ensure at least one row is present (helps with AJAX-loaded tables)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f"table.commonTable tbody tr")))
        table = container.find_element(By.CSS_SELECTOR, "table.commonTable")
        html = table.get_attribute("outerHTML")
        df = pd.read_html(StringIO(html))[0]
        return df
    except TimeoutException:
        print(f"Timed out: table under container '{content_name}' not found or no rows present.")
        return None
    except Exception as e:
        print(f"Error retrieving table for '{content_name}': {type(e).__name__}: {e}")
        return None
        



def get_unit(driver, wait):
    try:
        unit_element = wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), '币种：')]")))
        unit_text = unit_element.text
        return unit_text
    except TimeoutException:
        print("Timed out: unit element not found.")
        return None
    except Exception as e:
        print(f"Error retrieving unit: {type(e).__name__}: {e}")
        return None


def clean_df(df):
    if not isinstance(df.columns, pd.MultiIndex):
        return df
    df2 = df.copy()
    df2.columns = df2.columns.get_level_values(0)
    first_row = pd.Series(df.columns.get_level_values(1), index=df2.columns)
    df2 = pd.concat([first_row.to_frame().T, df2], ignore_index=True)
    return df2


def write_fail_stocks(fail_stocks):
    with open("fail_stocks.txt", "w") as f:
        for s in fail_stocks:
            f.write(s + "\n")

def append_fail_stocks(fail_stocks, stock_code, fail_reason):
    fail_stocks.append(stock_code)
    with open("fail_stocks.txt", "a") as f:
        f.write(f"{stock_code}: {fail_reason}\n")


# Load proxies
with open("valid_proxies.txt", "r") as f:
        proxies_list = [line.strip() for line in f if line.strip()]

# Load stock codes
stock_code_df = pd.read_csv("hk_stock_list_short.csv", dtype={"股份代號": str}) # whole stock list
last_stock = '08426'  # last scraped stock code
last_index = stock_code_df.loc[stock_code_df['股份代號'] == last_stock].index[0]
print(len(stock_code_df) - last_index - 1, "stocks remaining to scrape.")
stock_code_df = stock_code_df.iloc[last_index + 1: last_index + 201] # resume from next stock
stock_code_list = stock_code_df["股份代號"].tolist()

# Re-Scrape Code
# stock_code_list = ['01959', '01789', '01285', '01993', 
#                    '01962', '01948', '02016', '01973', '02023']
# stock_code_list = ['00798', '01218', '01905', '03399']

# Prepare data storage
content_list = ["content_zyzb", "content_zcfzb", "content_lrb", "content_xjllb"]
df_dict = {}
for c in content_list:
    try:
        df_existing = pd.read_csv(f"{c.split('_')[-1]}_data.csv")
        df_dict[c] = [df_existing]
    except FileNotFoundError:
        df_dict[c] = []

# Start scraping
success_stocks = []
fail_stocks = []
try_times = 1
stock_idx = 0
use_proxy_index = 0


# Main scraping loop
while stock_idx < len(stock_code_list):
    stock_code = stock_code_list[stock_idx]

    # Check if exceeded max retries
    if try_times > len(proxies_list):
        print(f"{stock_code}: Exceeded maximum retry attempts. try next stock.")
        append_fail_stocks(fail_stocks, stock_code, "Exceeded maximum retry attempts")
        stock_idx += 1
        try_times = 1
        continue
    
    # Wait a random time before each request
    random_number = random.randint(1, 5)
    time.sleep(random_number)
    
    # Set up driver with/without proxy
    proxy = proxies_list[use_proxy_index]
    try:
        driver = make_driver_with_proxy(proxy) # Scape with proxy
        # driver = make_driver_without_proxy() # Scrape without proxy
    except Exception as e:
        print(f"Error creating driver with proxy {proxy}: {e}")
        use_proxy_index += 1
        if use_proxy_index >= len(proxies_list):
            use_proxy_index = 0
        continue
    wait = WebDriverWait(driver, 15)  # increase timeout for slow pages

    print(f">>>>>Scraping stock {stock_idx+1}/{len(stock_code_list)}: {stock_code}, try_times: {try_times}， using proxy: {proxy}")

    # Start scraping
    try:
        open_url(driver, stock_code)
        page_html = driver.page_source

        # give up if fund page
        if "基金概况" in page_html and "基金代码" in page_html:
            print(f"{stock_code} is a fund, skipping.")
            append_fail_stocks(fail_stocks, stock_code, "Fund page")
            stock_idx += 1
            try_times = 1
            driver.quit()
            continue

        # give up if page not found
        empty_divs = driver.find_elements(By.CSS_SELECTOR, "div.empty")
        if empty_divs:
            print(f"{stock_code} page not found, skipping.")
            append_fail_stocks(fail_stocks, stock_code, "Page not found")
            stock_idx += 1
            try_times = 1
            driver.quit()
            continue
            
        unit_text = get_unit(driver, wait)
        unit = unit_text.split('：')[-1] if unit_text else None

        for content_name in content_list:
            table_name = content_name.split('_')[-1]
            df = get_finance_table(driver, wait, content_name)
            df_new = clean_df(df)
            df_transformed = tdfs.transform_data(df_new, stock_code, table_name, unit)
            exisiting_df = df_dict[content_name][0] if len(df_dict[content_name]) > 0 else pd.DataFrame()
            combined_df = pd.concat([exisiting_df, df_transformed], ignore_index=True)
            df_dict[content_name] = [combined_df]
            combined_df.to_csv(f"{table_name}_data.csv", index=False)
            print(f"Saved {table_name}_data.csv with {len(combined_df)} records")
            driver.execute_script("window.scrollBy(0, window.innerHeight/2);")
        
        success_stocks.append(stock_code)
        stock_idx += 1
        try_times = 1
        driver.quit()
    
    # Change Proxy upon failure
    except Exception as e:
        print(f"[!!!] Error loading page for {stock_code}: {e}")
        try_times += 1
        use_proxy_index += 1
        if use_proxy_index >= len(proxies_list):
            print("Ran out of proxies and rolling back to the first proxy.")
            use_proxy_index = 0
        driver.quit()
        continue

driver.quit()

print(f"Scraping completed. Successful stocks: {len(success_stocks)}, Failed stocks: {len(fail_stocks)}")