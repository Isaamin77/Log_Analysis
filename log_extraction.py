import re
import pandas as pd

# Load the log file
with open("logfile.log", "r", encoding="utf-8") as file:
    lines = file.readlines()

# Define regex patterns
timestamp_pattern = re.compile(r"\d{2}-\d{2}-\d{4}_\d{2}:\d{2}:\d{2}\.\d{3}")
error_pattern = re.compile(r"ERROR\s+(.*?)\s+-\s+(.*)")
sql_start_pattern = re.compile(r"^\s*Hibernate:\s*$", re.IGNORECASE)
sql_line_pattern = re.compile(r"^\s+(select|insert|update|delete)", re.IGNORECASE)
process_pattern = re.compile(r"(Process|ProcessRequest).*?[-]*\s*(start|end).*?:\s*(.*?):\s*(.*)", re.IGNORECASE)
open_close_pattern = re.compile(r"(Open-- count|Close-- Before\s*:|Close-- After\s*:\s*\d+)", re.IGNORECASE)

# Containers
error_entries = []
sql_queries = []
process_entries = []
open_close_entries = []

# SQL tracking
current_sql = []
capturing_sql = False
sql_timestamp = None
last_valid_timestamp = ""  

# Parse the log line by line
for idx, line in enumerate(lines):
    timestamp_match = timestamp_pattern.search(line)
    if timestamp_match:
        last_valid_timestamp = timestamp_match.group()  

    # ERRORs
    error_match = error_pattern.search(line)
    if error_match:
        error_entries.append({
            "Timestamp": last_valid_timestamp,
            "Source": error_match.group(1).strip(),
            "Message": error_match.group(2).strip(),
            "Line": idx + 1
        })

    # SQL Queries
    if sql_start_pattern.match(line):
        capturing_sql = True
        current_sql = []
        sql_timestamp = last_valid_timestamp  # <-- Use most recent timestamp
        continue

    if capturing_sql:
        if sql_line_pattern.match(line) or line.strip().lower().startswith(("select", "insert", "update", "delete")):
            current_sql.append(line.strip())
        elif line.strip() == "":
            continue
        else:
            if current_sql:
                sql_type = current_sql[0].split()[0].upper()
                sql_queries.append({
                    "Timestamp": sql_timestamp,
                    "SQL Type": sql_type,
                    "SQL": " ".join(current_sql),
                    "Line": idx + 1
                })
            current_sql = []
            capturing_sql = False

    # Process Entries
    process_match = process_pattern.search(line)
    if process_match:
        process_entries.append({
            "Timestamp": last_valid_timestamp,
            "Type": process_match.group(1),
            "Phase": process_match.group(2),
            "Component": process_match.group(3),
            "Process": process_match.group(4),
            "Line": idx + 1
        })

    # Open/Close Events
    open_close_match = open_close_pattern.search(line)
    if open_close_match:
        open_close_entries.append({
            "Timestamp": last_valid_timestamp,
            "Info": line.strip(),
            "Line": idx + 1
        })

# Save final SQL if still capturing
if current_sql:
    sql_type = current_sql[0].split()[0].upper()
    sql_queries.append({
        "Timestamp": sql_timestamp,
        "SQL Type": sql_type,
        "SQL": " ".join(current_sql),
        "Line": len(lines)
    })

# Export to CSV
pd.DataFrame(error_entries).to_csv("error_log_summary.csv", index=False)
pd.DataFrame(sql_queries).to_csv("sql_query_summary.csv", index=False)
pd.DataFrame(process_entries).to_csv("process_log_summary.csv", index=False)
pd.DataFrame(open_close_entries).to_csv("open_close_summary.csv", index=False)

# Confirmation
print(" Extraction complete.")
print(f" Errors: {len(error_entries)}")
print(f" SQL Queries: {len(sql_queries)}")
print(f" Process Entries: {len(process_entries)}")
print(f" Open/Close Logs: {len(open_close_entries)}")
