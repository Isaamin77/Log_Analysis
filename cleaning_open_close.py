import pandas as pd
import re

# Load the original file 
df = pd.read_csv("open_close_summary.csv")

# Parse and fix the timestamp 
def parse_timestamp(ts):
    for fmt in ("%d-%m-%Y_%H:%M:%S.%f", "%d-%m-%Y_%H:%M:%S"):
        try:
            return pd.to_datetime(ts, format=fmt)
        except:
            continue
    return pd.NaT

df['Timestamp'] = df['Timestamp'].apply(parse_timestamp)

# Extract readable fields from Info column 
def extract_open_close(info):
    info = str(info).strip()
    result = {"Action": None, "Before": None, "After": None}

    if "Open-- count" in info:
        result["Action"] = "Open Count"
        counts = re.findall(r"\d+", info)
        if counts:
            result["Before"] = counts[0]
            if len(counts) > 1:
                result["After"] = counts[1]
    elif "Close-- Before" in info:
        result["Action"] = "Close Count"
        before = re.search(r"Before\s*:\s*(\d+)", info)
        after = re.search(r"After\s*:\s*(\d+)", info)
        result["Before"] = before.group(1) if before else None
        result["After"] = after.group(1) if after else None
    else:
        result["Action"] = "Other"
    
    return pd.Series(result)

extracted = df["Info"].apply(extract_open_close)
df = pd.concat([df, extracted], axis=1)

# Save the cleaned version 
df.to_csv("cleaned_open_close_summary.csv", index=False)

print(" Cleaned file saved as 'cleaned_open_close_summary.csv'")
print(df.head())
