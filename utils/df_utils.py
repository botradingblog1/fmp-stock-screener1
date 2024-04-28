import pandas as pd
import numpy as np
from config import *
from sklearn.preprocessing import MinMaxScaler


def normalize_dataframe(df):
    column_list = df.columns
    scaler = MinMaxScaler()

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