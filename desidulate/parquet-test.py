import pandas as pd

# Load the Parquet file into a DataFrame
df = pd.read_parquet('/Volumes/Data/C64Music/MUSICIANS/H/Hubbard_Rob/Up_up_and_Away-01.parquet')

# Convert 'addr' and 'data' to hexadecimal format
df['addr'] = df['addr'].apply(lambda x: f"{x:04X}")
df['data'] = df['data'].apply(lambda x: f"{x:02X}")

# Display the data
print(df.info())

print(df[:30])
