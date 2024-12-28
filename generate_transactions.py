import csv
import random

# Constants
OUTPUT_FILE = "transactions.csv"
FILE_SIZE_GB = 2
ROW_SIZE_BYTES = 50  # Approximate size of each row in bytes
ROWS = (FILE_SIZE_GB * 1024**3) // ROW_SIZE_BYTES  # Total number of rows

# Transaction types and their characteristics
TRANSACTION_TYPES = [
    "deposit",        # Requires amount
    "withdrawal",     # Requires amount
    "dispute",        # Does not require amount
    "resolve",        # Does not require amount
    "chargeback",     # Does not require amount
]

def generate_transaction(row_id):
    """Generate a random transaction."""
    transaction_type = random.choice(TRANSACTION_TYPES)
    client_id = random.randint(1, 100_000)
    tx_id = row_id  # Use row_id as transaction ID to ensure uniqueness
    if transaction_type in ["deposit", "withdrawal"]:
        amount = round(random.uniform(1.0, 10_000.0), 2)  # Random amount
        return [transaction_type, client_id, tx_id, amount]
    else:
        return [transaction_type, client_id, tx_id, ""]

def main():
    with open(OUTPUT_FILE, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["type", "client", "tx", "amount"])  # CSV header

        for row_id in range(1, ROWS + 1):
            writer.writerow(generate_transaction(row_id))
            if row_id % 1_000_000 == 0:
                print(f"Generated {row_id:,} rows...")

    print(f"Done! Generated {ROWS:,} rows in {OUTPUT_FILE}.")

if __name__ == "__main__":
    main()
