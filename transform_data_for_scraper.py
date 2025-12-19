import pandas as pd
import numpy as np


def reset_header(df):
    """
    Reset the header of the dataframe using the first row as the new header.
    
    :param df: Input DataFrame
    :return: DataFrame with reset header
    """
    new_header = df.iloc[0]  # first row as header
    df.columns = new_header  # set the header row as the dataframe header
    return df.loc[1:].reset_index(drop=True)  # drop the first row and reset index


def add_prefixes(df):
    """
    Add prefixes to the first column of the dataframe based on hierarchical structure.

    :param df: Input DataFrame
    :return: DataFrame with updated first column
    """
    prefix = ''
    drop_i = []

    for i in range(len(df)):
        col = df.iloc[i, 0]
        val = df.iloc[i, 1]
        if val == col:
            prefix = col
            drop_i.append(i)
            continue

        elif (col == prefix) or ('总额' in col):
            prefix = ''

        if prefix != '':
            df.iloc[i, 0] = f"{prefix}_{col}"

    keep_i = [i for i in range(len(df)) if i not in drop_i]
    df = df.iloc[keep_i].reset_index(drop=True)
    return df


def transform_data(df, stock_code, type_name, unit=None):
    """
    Transform the financial data into a long format with additional columns.

    :param df: Input DataFrame
    :param stock_code: Stock code
    :param type_name: Type of financial data (e.g., 'lrb', 'xjllb', 'zyzb')
    """
    index_col = ['截止日期']
    if type_name in ['lrb', 'xjllb']:
        df = reset_header(df)
        index_col = ['报表截止日']
    
    df = add_prefixes(df) # add prefixes to first column
    df_t = df.set_index(index_col).T # transpose
    print(df_t.head())
    df_t.reset_index(names=index_col, inplace=True) # reset index

    # melt the dataframe
    id_vars = index_col if type_name == 'zyzb' else index_col + ['年结日']
    df_final = pd.melt(df_t, id_vars=id_vars, var_name='指标', value_name='数值')

    # split '指标' into '指标组' and '指标名称'
    parts = df_final['指标'].str.partition('_')
    has_sep = parts[1] == '_'
    df_final['指标组'] = np.where(has_sep, parts[0], df_final['指标'])
    df_final['指标名称'] = np.where(has_sep, parts[2], df_final['指标'])
    df_final = df_final.drop(columns=['指标'])

    df_final['股票代码'] = stock_code
    df_final['币种'] = unit
    return df_final

