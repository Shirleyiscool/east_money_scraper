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

options = Options()
options.add_argument("--headless=new")  # headless mode
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, 15)  # increase timeout for slow pages


def open_url(driver, stock_code):
    url = f"https://emweb.securities.eastmoney.com/PC_HKF10/pages/home/index.html?code={stock_code}&type=web&color=w#/NewFinancialAnalysis"
    driver.get(url)


def get_finance_table(driver, wait, content_name="content_zyzb"):
    try:
        # Wait for the table element to exist in the DOM
        container = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f"div.content.{content_name}")))
        table = container.find_element(By.CSS_SELECTOR, "table.commonTable")

        # Optionally ensure at least one row is present (helps with AJAX-loaded tables)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f"table.commonTable tbody tr")))
        html = table.get_attribute("outerHTML")
        df = pd.read_html(StringIO(html))[0]
        return df

    except TimeoutException:
        print(f"Timed out: table under container '{content_name}' not found or no rows present.")
        # handle retry / save screenshot / quit driver
        driver.quit()



def get_unit(driver, wait):
    try:
        unit_element = wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), '币种：')]")))
        unit_text = unit_element.text
        return unit_text
    except TimeoutException:
        print("Timed out: unit element not found.")
        driver.quit()


def clean_df(df):
    if not isinstance(df.columns, pd.MultiIndex):
        return df
    df2 = df.copy()
    df2.columns = df2.columns.get_level_values(0)
    first_row = pd.Series(df.columns.get_level_values(1), index=df2.columns)
    df2 = pd.concat([first_row.to_frame().T, df2], ignore_index=True)
    return df2


stock_code_list = ["00700", "01398"]  # Example stock code
content_list = ["content_zyzb", "content_zcfzb", "content_lrb", "content_xjllb"]
df_dict = {c: None for c in content_list}

for i, stock_code in enumerate(stock_code_list): 
    open_url(driver, stock_code)
    unit_text = get_unit(driver, wait)
    unit = unit_text.split('：')[-1] if unit_text else None
    print(stock_code, unit)
    for content_name in content_list:
        df = get_finance_table(driver, wait, content_name)
        df_new = clean_df(df)
        df_transformed = tdfs.transform_data(df_new, stock_code, content_name.split('_')[-1], unit)
        print(f"Transformed Data for {content_name}:\n", df_transformed.head())
        if i == 0:
            combined_df = df_transformed
            df_dict[content_name] = df_transformed
        else:
            combined_df = pd.concat([df_dict[content_name], df_transformed])
            
        combined_df.to_csv(f"{content_name}_data.csv", index=False)
        print(f"Saved {content_name}_data.csv with {len(combined_df)} records")

driver.quit()
