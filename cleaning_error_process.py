import pandas as pd
import re

# Define simplification rules for 'Component' or 'Source' fields
def simplify_component(component):
    if pd.isna(component):
        return ""
    # Take last segment and add space between capital words
    simple = component.split('.')[-1]
    return re.sub(r'(?<!^)(?=[A-Z])', ' ', simple).strip()

# Function to convert timestamp column
def clean_timestamp(df, timestamp_col):
    df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce', format="%d-%m-%Y_%H:%M:%S.%f")
    return df

#Load CSVs
error_df = pd.read_csv("updated_error_log_summary.csv")
process_df = pd.read_csv("updated_process_log_summary.csv")

# Clean Error Log
error_df = clean_timestamp(error_df, "Timestamp")
error_df["Source"] = error_df["Source"].apply(simplify_component)

# Clean Process Log
process_df = clean_timestamp(process_df, "Timestamp")
process_df["Component"] = process_df["Component"].apply(simplify_component)

# Save cleaned CSVs
error_df.to_csv("cleaned_error_log_summary.csv", index=False)
process_df.to_csv("cleaned_process_log_summary.csv", index=False)

print(" CSV file cleaned and saved.")
