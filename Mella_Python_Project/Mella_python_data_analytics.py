"""
Mella_python_data_analytics.py
----------------------------------
Reusable functions for Data Analytics:
- Reading multiple CSV files
- Cleaning (NaN, normalization, regex, datetime conversion, duplicate removal)
- Summary statistics
- Plotting
- SQL Server integration
- This is a final project developed by Firts Batch Mella-Python Data Analytics Group, September, 2025
""" 
import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import pyodbc
from sqlalchemy import create_engine
import numpy as np
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# 1. Read multiple CSV files
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
def read_multiple_csv(folder_path):
    """Reads all CSV files in a folder and concatenates into one DataFrame."""
    dataframes = []
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder_path, filename)
            df = pd.read_csv(file_path)
            dataframes.append(df)
    if dataframes:
        return pd.concat(dataframes, ignore_index=True)
    else:
        return pd.DataFrame()
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
def read_individual_csv_files(folder_path):
    """Reads all CSV files in a folder and returns them as a dictionary."""
    files_data = {}
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder_path, filename)
            df = pd.read_csv(file_path)
            files_data[filename] = df
    return files_data
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# 2. Cleaning functions
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
def handle_missing_values(df, method="drop"):
    """Handle NaN values by drop, fill (0), or forward fill."""
    if method == "drop":
        return df.dropna()
    elif method == "fill":
        return df.fillna(0)
    elif method == "ffill":
        return df.fillna(method="ffill")
    return df

def convert_to_numeric(df, column):
    """Convert a column to numeric, handling errors by coercing to NaN."""
    df[column] = pd.to_numeric(df[column], errors='coerce')
    return df

def normalize_column(df, column, method="minmax"):

    df = convert_to_numeric(df, column)

    if method == "minmax":
        min_val = df[column].min()
        max_val = df[column].max()
        if max_val != min_val and not pd.isna(min_val) and not pd.isna(max_val):
            df[column] = (df[column] - min_val) / (max_val - min_val)
        else:
            df[column] = 0
        print(f"Min-Max normalized column '{column}' (range: 0-1)")

    elif method == "zscore":
        mean_val = df[column].mean()
        std_val = df[column].std()
        if std_val != 0 and not pd.isna(mean_val) and not pd.isna(std_val):
            df[column] = (df[column] - mean_val) / std_val
        else:
            df[column] = 0
        print(f"Z-score normalized column '{column}' (mean=0, std=1)")

    else:
        raise ValueError("Method must be 'minmax' or 'zscore'")

    return df

def regex_clean_column(df, column, chars_to_remove):
    """Remove specified characters from a column using regex."""
    pattern = f"[{re.escape(chars_to_remove)}]"
    df[column] = df[column].astype(str).replace(pattern, "", regex=True)
    return df

def convert_datetime_column(df, column, format="%Y-%m-%d", errors="coerce"):
    """
    Convert a column to datetime using specified format.
    """
    try:
        df[column] = pd.to_datetime(df[column], format=format, errors=errors)
        print(f"Successfully converted column '{column}' to datetime using format '{format}'")
    except Exception as e:
        print(f"Error converting column '{column}': {e}")
    return df

def drop_duplicates(df, subset=None, keep='first', inplace=False):
    if inplace:
        result = df.drop_duplicates(subset=subset, keep=keep, inplace=True)
        print(f"Removed duplicates in-place. Remaining rows: {len(df)}")
        return None
    else:
        result = df.drop_duplicates(subset=subset, keep=keep, inplace=False)
        print(f"Removed {len(df) - len(result)} duplicate rows. Remaining rows: {len(result)}")
        return result
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# 3. Statistics functions
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
def get_summary_stats(df, column):
    df_temp = df.copy()
    df_temp = convert_to_numeric(df_temp, column)
    numeric_values = df_temp[column].dropna()

    if len(numeric_values) == 0:
        return {"error": f"Column '{column}' contains no numeric values after conversion"}

    return {
        "min": numeric_values.min(),
        "max": numeric_values.max(),
        "mean": numeric_values.mean(),
        "median": numeric_values.median(),
        "std": numeric_values.std(),
        "count": len(numeric_values),
        "null_count": len(df_temp[column]) - len(numeric_values)
    }
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# 4. Plotting function
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
def plot_column(df, column, kind="hist"):
    df_temp = df.copy()
    df_temp = convert_to_numeric(df_temp, column)
    numeric_values = df_temp[column].dropna()

    if len(numeric_values) == 0:
        print(f"Cannot plot column '{column}' - no numeric values")
        return

    if kind == "hist":
        numeric_values.plot(kind="hist", bins=20, title=f"Histogram of {column}")
    elif kind == "box":
        numeric_values.plot(kind="box", title=f"Boxplot of {column}")
    elif kind == "bar":
        df[column].value_counts().plot(kind="bar", title=f"Bar Chart of {column}")
    plt.show()
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# 5. Database functions
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
def export_to_mssql(df, table_name, connection_string):
    """Export DataFrame to MSSQL using SQLAlchemy."""
    engine = create_engine(connection_string)
    df.to_sql(table_name, engine, if_exists="replace", index=False)
def read_from_mssql(table_name, connection_string):
    """Read table from MSSQL into pandas DataFrame."""
    engine = create_engine(connection_string)
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, engine)
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# 6. Data exploration functions
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
def explore_dataframe(df):
    """Print comprehensive information about the DataFrame."""
    print("=== DATAFRAME EXPLORATION ===")
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print("\nData types:")
    print(df.dtypes)
    print("\nMissing values per column:")
    print(df.isnull().sum())
    print("\nBasic statistics for numeric columns:")
    print(df.describe())
def identify_numeric_columns(df):
    """Identify and return numeric columns with mixed data types."""
    numeric_cols = []
    mixed_cols = []

    for col in df.columns:

        numeric_series = pd.to_numeric(df[col], errors='coerce')
        non_null_count = numeric_series.notna().sum()

        if non_null_count > 0:  
            if non_null_count == len(df):  
                numeric_cols.append(col)
            else:  # Mixed data types
                mixed_cols.append((col, non_null_count, len(df) - non_null_count))

    return numeric_cols, mixed_cols
# THE END