import pandas as pd
import numpy as np
import transform_stock_code as tsc

def clean_zyzb_df(df):
    """
    重要指标表清理函数：截止日期,数值,指标组,指标名称,股票代码,币种
    
    :param df: Description
    """
    df = tsc.transform_stock_code(df, '股票代码')
    df = df.drop_duplicates()
    val_col = '数值'
    val_string = df[val_col].str.extract(r'(-?\d+(?:\.\d+)?)%?')[0]
    df['value'] = pd.to_numeric(val_string, errors='coerce')
    df['unit'] = df[val_col].str.replace(r'(-?\d+(?:\.\d+)?)%?', '')
    print(df.head(20))

zyzb_df = pd.read_csv("zyzb_data_00210.csv")
clean_zyzb_df(zyzb_df)