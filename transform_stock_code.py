import pandas as pd


def transform_stock_code(df, stock_code_col):
    """
    Transform stock codes in the specified column to a standardized 5-digit format.

    :param df: Input DataFrame
    :param stock_code_col: Column name containing stock codes
    :return: DataFrame with transformed stock codes
    """
    def format_code(code):
        code_str = str(code).strip()
        if code_str.isdigit():
            return code_str.zfill(5)
        return code_str

    df[stock_code_col] = df[stock_code_col].apply(format_code)
    return df



if __name__ == "__main__":
    # Example usage
    df = pd.read_csv("hk_stock_list_short.csv")
    print("Before Transformation:")
    print(df.head())

    transformed_df = transform_stock_code(df, '股份代號')
    print("\nAfter Transformation:")
    print(transformed_df)
    transformed_df.to_csv("hk_stock_list_short.csv", index=False)