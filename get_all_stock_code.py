from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from io import StringIO
import time
import pandas as pd

options = Options()
options.add_argument("--headless=new")  # headless mode
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, 60)  # increase timeout for slow pages


def open_url(driver):
    url = f"https://www.hkexnews.hk/stocklist_active_main_c.htm"
    driver.get(url)


def load_all_rows_by_scrolling(driver, pause=1.0, max_iter=100):
    prev = -1
    for _ in range(max_iter):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)
        rows = driver.find_elements(By.CSS_SELECTOR, "table.table-stocklist tbody tr")
        if len(rows) == prev:
            break
        prev = len(rows)
    return rows


def get_all_stock_codes(driver, wait):
    try:
        # Wait for the stock list table to load
        load_all_rows_by_scrolling(driver)
        table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-stocklist")))
        print(table)
        html = table.get_attribute("outerHTML")
        df = pd.read_html(StringIO(html))[0]
        print(len(df))
        print(df.head())
        df.to_csv("hk_stock_list.csv", index=False)
        return df

    except TimeoutException:
        print("Timed out: stock list table not found.")
        page_html = driver.page_source
        print(page_html)
        driver.quit()
    return None

open_url(driver)
stock_df = get_all_stock_codes(driver, wait)