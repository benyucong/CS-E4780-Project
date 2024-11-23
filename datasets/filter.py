import pandas as pd

# Load the dataset
csv_file = "debs2022-gc-trading-day-08-11-21.csv"
data = pd.read_csv(csv_file, comment='#', delimiter=",", index_col=False)

# Filter rows where 'Trading time' and 'Last' columns are not empty
filtered_data = data[
    data['Trading time'].notna() & (data['Trading time'] != "") &
    data['Last'].notna() & (data['Last'] != "")
]

# Save the filtered data to a new CSV file (optional)
filtered_data.to_csv("filtered_trading_data_Monday.csv", index=False)

print(filtered_data)