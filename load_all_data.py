import os
import subprocess
from pathlib import Path

# List of known crypto symbols to help with asset type classification
KNOWN_CRYPTO = [
    "BTC", "ETH", "XRP", "LTC", "BCH", "ADA", "DOT", "LINK", "XLM",
    "USDC", "UNI", "DOGE", "WBTC", "AAVE", "ATOM", "SOL", "ETC",
    "FIL", "TRX", "XMR", "XTZ", "EOS", "MKR", "SNX", "COMP", "SUSHI",
    "YFI", "ZRX", "MANA", "STORJ", "FLR", "ALGO", "MATIC", "DAI",
    "ZEC", "BAT", "AVAX", "CELO"
]

def run_command(command):
    """Executes a command and logs its output."""
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=True,
        encoding='utf-8',
        errors='replace'
    )
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())
    return process.poll()

def load_data():
    """Iterates through files and loads them into the database."""
    data_dir = Path("data/stock_and_crypto_data")
    files_to_load = sorted([f for f in data_dir.iterdir() if f.is_file() and f.suffix == '.csv'])
    total_files = len(files_to_load)

    print(f"Found {total_files} CSV files to load.")

    for i, file_path in enumerate(files_to_load):
        symbol = file_path.stem.upper().replace('-USD', '')
        
        asset_type = "CRYPTO" if symbol in KNOWN_CRYPTO else "STOCK"
        
        command = (
            f"python -m etl.cli load '{file_path}' "
            f"--symbol {symbol} --asset-type {asset_type}"
        )

        print(f"\n--- Loading file {i+1}/{total_files}: {file_path} ---")
        print(f"Executing: {command}")
        
        return_code = run_command(command)
        
        if return_code == 0:
            print(f"--- Successfully loaded {file_path} ---")
        else:
            print(f"*** ERROR loading {file_path}. Return code: {return_code} ***")

if __name__ == "__main__":
    load_data() 