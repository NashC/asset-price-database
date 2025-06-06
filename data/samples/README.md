# Sample Data Files

This folder contains sample price data files for testing and demonstration purposes.

## Files Included

| Symbol | Company/Asset | Type | Description |
|--------|---------------|------|-------------|
| **BTC** | Bitcoin | Cryptocurrency | Leading cryptocurrency |
| **ETH** | Ethereum | Cryptocurrency | Second-largest cryptocurrency |
| **ABNB** | Airbnb | Stock | Travel/hospitality platform |
| **ADBE** | Adobe | Stock | Creative software company |
| **CRM** | Salesforce | Stock | Cloud-based CRM platform |
| **JNJ** | Johnson & Johnson | Stock | Healthcare/pharmaceutical |

## Data Format

Each CSV file contains daily OHLCV (Open, High, Low, Close, Volume) data with additional metadata:

```csv
Date,Open,High,Low,Close,Volume,Symbol,Asset_Type,Data_Source,Download_Date,Adj_Close,Source,Transform_Date,Data_Start_Date,Data_End_Date,Record_Count,Data_Quality_Score,YFinance_Version,Exchange,Trading_Pair
```

## Usage

These files can be used to:

1. **Test the ETL pipeline**:
   ```bash
   apdb load data/samples/BTC.csv --symbol BTC --asset-type CRYPTO
   ```

2. **Validate data quality**:
   ```bash
   apdb validate data/samples/ADBE.csv
   ```

3. **Demonstrate the system** without requiring the full dataset

## Asset Types

- **CRYPTO**: BTC, ETH
- **STOCK**: ABNB, ADBE, CRM, JNJ

## Data Source

All sample files are sourced from the same provider as the main dataset and follow the same format and quality standards. 