import pandas as pd

# Read parquet file
df = pd.read_parquet("yellow_tripdata_2025-05Big.parquet")

# Take random sample
sample = df.sample(n=10000, random_state=42)

# Save the sample
sample.to_parquet("yellow_tripdata_2025-05.parquet")