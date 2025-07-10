import pandas as pd

# Load individual cleaned CSVs
error_df = pd.read_csv("cleaned_error_log_summary.csv")
process_df = pd.read_csv("cleaned_process_log_summary.csv")
sql_df = pd.read_csv("sql_query_summary.csv")
open_close_df = pd.read_csv("cleaned_open_close_summary.csv")

# Add LogType column
error_df['LogType'] = 'Error'
process_df['LogType'] = 'Process'
sql_df['LogType'] = 'SQL'
open_close_df['LogType'] = 'Open/Close'

# Combine logs
combined_df = pd.concat([error_df, process_df, sql_df, open_close_df], ignore_index=True)
combined_df = combined_df.fillna("N/A")



# Count of logs by type
print("Log Type Counts:")
print(combined_df['LogType'].value_counts())
print()

# Top error sources
if 'Source' in combined_df.columns:
    top_sources = (
        combined_df[combined_df['LogType'] == 'Error']
        .groupby('Source')
        .size()
        .sort_values(ascending=False)
        .head(10)
    )
    print("Top Error Sources:")
    print(top_sources)
    print()
else:
    print(" 'Source' column not found for Error logs.\n")


# Processes most linked to errors by timestamp
if 'Process' in process_df.columns:
    merged = (
        process_df[['Timestamp', 'Process']]
        .merge(error_df[['Timestamp', 'Message']], on="Timestamp", how="inner")
    )
    top_processes_with_errors = (
        merged.groupby('Process')
        .size()
        .sort_values(ascending=False)
        .head(10)
    )
    print("Processes Most Linked to Errors:")
    print(top_processes_with_errors)
    print()
else:
    print(" 'Process' column not available in process logs.\n")
