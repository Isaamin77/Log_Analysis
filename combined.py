import pandas as pd

# === Load all cleaned CSVs ===
error_df = pd.read_csv("cleaned_error_log_summary.csv")
sql_df = pd.read_csv("sql_query_summary.csv")
process_df = pd.read_csv("cleaned_process_log_summary.csv")
open_close_df = pd.read_csv("cleaned_open_close_summary.csv")

# === Standardize common columns ===
error_df["Log Type"] = "ERROR"
sql_df["Log Type"] = "SQL"
process_df["Log Type"] = "PROCESS"
open_close_df["Log Type"] = "OPEN_CLOSE"

# Add missing columns to align all DataFrames
for df in [error_df, sql_df, process_df, open_close_df]:
    for col in ["Timestamp", "Component", "Message", "SQL", "Phase", "Process", "Source", "Info"]:
        if col not in df.columns:
            df[col] = None  # Fill missing columns with None for consistency

# Reorder columns for consistency
columns_order = ["Timestamp", "Log Type", "Component", "Phase", "Process", "SQL", "Source", "Message", "Info", "Line"]

error_df = error_df[columns_order]
sql_df = sql_df[columns_order]
process_df = process_df[columns_order]
open_close_df = open_close_df[columns_order]

# === Combine all logs into one DataFrame ===
combined_df = pd.concat([error_df, sql_df, process_df, open_close_df], ignore_index=True)

# Optional: Sort by timestamp if needed
combined_df["Timestamp"] = pd.to_datetime(combined_df["Timestamp"], errors="coerce")
combined_df = combined_df.sort_values(by="Timestamp")

# === Export to CSV ===
combined_df.to_csv("final_combined_log_summary.csv", index=False)

print("✅ Combined log summary saved to 'final_combined_log_summary.csv'")
print("✔️ Total rows combined:", len(combined_df))
