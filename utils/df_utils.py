import pandas as pd
import numpy as np
from config import *
from sklearn.preprocessing import MinMaxScaler


def normalize_dataframe(df):
    column_list = df.columns
    scaler = MinMaxScaler(feature_range=(0, 100))

    # Check if columns exist and if they have more than one unique value
    for col in column_list:
        if col == 'symbol':
            continue
        if col in df.columns and len(df[col].dropna().unique()) > 1:
            # Reshape data for scaler
            df[col] = scaler.fit_transform(df[[col]])  # Double brackets keep the DataFrame structure
        else:
            print(f"Warning: Column {col} not found or not enough data to scale in DataFrame")

    return df


def round_dataframe_columns(df, precision=4):
    column_list = df.columns

    # Check if columns exist
    for col in column_list:
        if col == 'symbol':
            continue
        if col in df.columns:
            # Reshape data for scaler
            df[col] = round(df[col], precision)
        else:
            print(f"Warning: Column {col} not found or not enough data to scale in DataFrame")

    return df


def cap_outliers(df, column_name):
    # Calculate mean and standard deviation
    mean_factor = df[column_name].mean()
    std_factor = df[column_name].std()

    # Define cutoffs for outliers
    upper_limit = mean_factor + OUTLIER_STD_MULTIPLIER * std_factor
    lower_limit = mean_factor - OUTLIER_STD_MULTIPLIER * std_factor

    # Cap values
    df[column_name] = np.where(df[column_name] > upper_limit, upper_limit, df[column_name])
    df[column_name] = np.where(df[column_name] < lower_limit, lower_limit, df[column_name])

    return df


def merge_dataframes(symbol_list, df_list):
    # Initialize the merged dataframe with the symbol list to ensure all symbols are included
    merged_df = pd.DataFrame(symbol_list, columns=['symbol'])

    # Merge each dataframe one by one
    for df in df_list:
        # Ensure that the merging dataframe has the 'symbol' column
        if 'symbol' in df.columns:
            merged_df = pd.merge(merged_df, df, on='symbol', how='left')
        else:
            print("Warning: DataFrame missing 'symbol' column, skipping...")

    return merged_df


def merge_dataframes_how(df_list, how='inner'):
    # Merge each dataframe one by one
    merged_df = pd.DataFrame({'symbol': df_list[0]['symbol']})
    for df in df_list:
        # Ensure that the merging dataframe has the 'symbol' column
        if 'symbol' in df.columns:
            merged_df = pd.merge(merged_df, df, on='symbol', how=how)
        else:
            print("Warning: DataFrame missing 'symbol' column, skipping...")

    return merged_df