import pandas as pd
import numpy as np
import re

print("Starting ETL Pipeline: EDA & Data Cleaning\n")

# --- 1. EXTRACT: Load the Anonymized Data ---
try:
    meta_df = pd.read_csv('sample_meta_data_secure.csv')
    web_df = pd.read_csv('sample_website_data_secure.csv')
    crm_df = pd.read_csv('sample_crm_data_secure.csv')
    print("Files loaded successfully.\n")
except FileNotFoundError:
    print("Error: Secure sample files not found.")
    exit()

# --- 2. EXPLORATORY DATA ANALYSIS (EDA) ---
print("--- EDA: Missing Values Check ---")
print(f"Meta Ads Missing Values:\n{meta_df.isnull().sum()}\n")
print(f"Website Missing Values:\n{web_df.isnull().sum()}\n")
print(f"CRM Missing Values:\n{crm_df.isnull().sum()}\n")

# --- 3. TRANSFORM: Cleaning Functions ---

def standardize_dates(date_series):
    """
    Strips ordinal suffixes (st, nd, rd, th) and standardizes to YYYY-MM-DD.
    SQL databases require standard ISO date formats for accurate time-series analysis.
    """
    # Regex to remove ordinal indicators attached to numbers
    cleaned_series = date_series.astype(str).str.replace(r'(?<=\d)(st|nd|rd|th)\b', '', regex=True)
    # Parse mixed formats and convert to standard YYYY-MM-DD
    return pd.to_datetime(cleaned_series, format='mixed', dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')

def clean_strings(text_series):
    """Converts strings to Title Case and strips leading/trailing whitespaces."""
    return text_series.astype(str).str.title().str.strip()

# --- 4. APPLY TRANSFORMATIONS ---

print("Cleaning Meta Ads Data...")
meta_df['Lead_Date'] = standardize_dates(meta_df['Lead_Date'])
meta_df['Raw_Name'] = clean_strings(meta_df['Raw_Name'])
meta_df['Campaign_Name'] = meta_df['Campaign_Name'].str.strip() # Keep original casing for campaigns

print("Cleaning Website Data...")
web_df['Session_Date'] = standardize_dates(web_df['Session_Date'])
web_df['UTM_Source'] = web_df['UTM_Source'].str.strip()

print("Cleaning CRM Sales Data...")
crm_df['Close_Date'] = standardize_dates(crm_df['Close_Date'])
crm_df['Original_Lead_Source'] = crm_df['Original_Lead_Source'].str.strip()

# --- 5. HANDLING BUSINESS ANOMALIES ---

print("Resolving Data Quality Issues...")

# A. Handle Missing Financials (The 4% Anomaly)
# We will impute missing values using the median contract value of the respective brand
median_24f = crm_df[crm_df['Brand_Booked'] == '24 Frames']['Final_Value_INR'].median()
median_vs = crm_df[crm_df['Brand_Booked'] == 'Visual Stories']['Final_Value_INR'].median()

crm_df.loc[(crm_df['Final_Value_INR'].isnull()) & (crm_df['Brand_Booked'] == '24 Frames'), 'Final_Value_INR'] = median_24f
crm_df.loc[(crm_df['Final_Value_INR'].isnull()) & (crm_df['Brand_Booked'] == 'Visual Stories'), 'Final_Value_INR'] = median_vs

# B. Handle "Time Travel" Anomalies (Close Date < Lead Date)
# To do this, we need to temporarily merge the CRM dates with the Lead dates
# First, create a master lead date lookup
meta_dates = meta_df[['Phone_Number', 'Lead_Date']].rename(columns={'Lead_Date': 'Initial_Date'})
web_dates = web_df[['Phone_Number', 'Session_Date']].rename(columns={'Session_Date': 'Initial_Date'})
master_dates = pd.concat([meta_dates, web_dates]).drop_duplicates(subset=['Phone_Number'])

# Merge to compare
crm_df = crm_df.merge(master_dates, on='Phone_Number', how='left')

# Convert back to datetime for comparison
crm_df['Close_Date_DT'] = pd.to_datetime(crm_df['Close_Date'])
crm_df['Initial_Date_DT'] = pd.to_datetime(crm_df['Initial_Date'])

# Create a Data Quality Flag
crm_df['Is_Valid_Sales_Cycle'] = (crm_df['Close_Date_DT'] >= crm_df['Initial_Date_DT'])

# Calculate the Sales Cycle in Days (setting invalid ones to NaN or 0)
crm_df['Sales_Cycle_Days'] = (crm_df['Close_Date_DT'] - crm_df['Initial_Date_DT']).dt.days
crm_df.loc[crm_df['Is_Valid_Sales_Cycle'] == False, 'Sales_Cycle_Days'] = np.nan

# Drop the temporary calculation columns
crm_df = crm_df.drop(columns=['Initial_Date', 'Close_Date_DT', 'Initial_Date_DT'])

# --- 6. LOAD: Export Cleaned Data ---
meta_df.to_csv('clean_meta_data.csv', index=False)
web_df.to_csv('clean_website_data.csv', index=False)
crm_df.to_csv('clean_crm_data.csv', index=False)

print("\nPipeline Complete! Clean datasets ready for SQL:")
print("- clean_meta_data.csv")
print("- clean_website_data.csv")
print("- clean_crm_data.csv")
