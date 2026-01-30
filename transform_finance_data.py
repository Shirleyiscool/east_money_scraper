import pandas as pd
import numpy as np
import transform_stock_code as tsc
import re
import os

def output_date_df(df, content_name='lrb'):
    df = tsc.transform_stock_code(df, '股票代码')
    df['年结月'] = df['年结日'].astype(str).str.split('-').str[0].astype(pd.Int64Dtype())
    df_1 = df[['股票代码', '年结月']].drop_duplicates()
    df_2 = df.groupby('股票代码')['报表截止日'].max().reset_index()
    date_df = pd.merge(df_2, df_1, on='股票代码', how='left')
    date_df.rename(columns={'报表截止日': '最新报表截止日'}, inplace=True)
    return date_df


def extract_chinese(text):
    # Regex pattern for Chinese characters (CJK Unified Ideographs range)
    chinese_pattern = re.compile(r'[\u4E00-\u9FFF]+') # '+' matches one or more consecutive Chinese chars
    return "".join(chinese_pattern.findall(text)) # Joins the list of found characters into a single string


def clean_df(df, content_name):
    """
    利润表：报表截止日,年结日,数值,指标组,指标名称,股票代码,币种
    现金流量表：报表截止日,年结日,数值,指标组,指标名称,股票代码,币种
    资产负债表：截止日期,年结日,数值,指标组,指标名称,股票代码,币种
    重要指标：截止日期,数值,指标组,指标名称,股票代码,币种
    """
    df = tsc.transform_stock_code(df, '股票代码')
    df = df.drop_duplicates()

    # 处理日期相关字段
    if content_name in ['zcfzb', 'zyzb']:
        df.rename(columns={'截止日期': '报表截止日'}, inplace=True)
    df['报表截止日'] = pd.to_datetime('20'+ df['报表截止日'].astype(str), errors='coerce').dt.date
    df['报表月'] = pd.to_datetime(df['报表截止日']).dt.month.astype(pd.Int64Dtype())
    if '年结日' in df.columns:
        df['年结月'] = df['年结日'].astype(str).str.split('-').str[0].astype(pd.Int64Dtype())
        df['是否年报'] = np.where(df['报表月'] == df['年结月'], 1, 0)

    # 处理数值和单位
    val_col = '数值'
    # Apply the function to the 'val_col' to create a new column with only Chinese characters
    df['unit'] = df[val_col].apply(extract_chinese)
    df['value'] = df[val_col].str.replace(r'[\u4E00-\u9FFF]+', '', regex=True)
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df.loc[df['unit']=='万亿', 'value'] = df['value'] * 1000000000000
    df.loc[df['unit']=='亿', 'value'] = df['value'] * 100000000
    df.loc[df['unit']=='万', 'value'] = df['value'] * 10000

    # 指标组,指标名称 排序
    index_name_df = df[['指标组', '指标名称']].drop_duplicates().reset_index(drop=True).reset_index().rename(columns={'index': 'order_index'})
    output_df = pd.merge(df, index_name_df, on=['指标组', '指标名称'], how='left')

    return output_df


if __name__ == "__main__":
    input_path = 'finance_data/'
    output_path = 'transformed_finance_data/'

    stock_df = pd.read_csv('hk_stock_list_short.csv', dtype={"股份代號": str})

    # sort out file_type and files
    file_name_list = os.listdir(input_path)
    file_type_list = ['lrb', 'zcfzb', 'xjllb', 'zyzb']
    file_dict = {file_type: [] for file_type in file_type_list}
    for file_name in file_name_list:
        file_type = file_name.split('_')[0]
        file_dict[file_type].append(file_name)

    # Transform and Combine all files within a file type
    for file_type in file_type_list:
        file_list = file_dict[file_type]
        combined_df = pd.DataFrame()

        for file_name in file_list:
            df = pd.read_csv(os.path.join(input_path, file_name))
            combined_df = pd.concat([combined_df, clean_df(df, file_type)])

        if file_type == 'lrb':
            date_df = output_date_df(combined_df)
            date_df.to_csv(os.path.join(output_path, 'stock_date_df.csv'), index=False)
        
        if file_type == 'zyzb':
            combined_df = pd.merge(combined_df, date_df, on='股票代码', how='left')
            combined_df['是否年报'] = np.where(combined_df['报表月'] == combined_df['年结月'], 1, 0)
        else:
            combined_df = pd.merge(combined_df, date_df[['股票代码', '最新报表截止日']], on='股票代码', how='left')
        
        print(file_type, ':', len(combined_df))
        combined_df['是否最新报表'] = np.where(combined_df['报表截止日'] == combined_df['最新报表截止日'], 1, 0)
        output_df = combined_df.loc[(combined_df['是否最新报表']==1) | (combined_df['是否年报']==1)]
        output_df = pd.merge(output_df, stock_df, left_on='股票代码', right_on='股份代號')
        save_df = output_df.sort_values(by=['股票代码','报表截止日', 'order_index'], ascending=[True, False, True]).drop_duplicates()
        
        output_columns = ['股票代码', '股份簡稱', '报表截止日', '年结月', '是否年报', '是否最新报表','指标组', '指标名称', '币种', '数值', 'value', 'order_index']
        print(file_type, ':', len(save_df[output_columns]))
        print(save_df[output_columns].head(5))
        save_df[output_columns].to_csv(os.path.join(output_path, f'{file_type}_data.csv'), index=False)
        



    