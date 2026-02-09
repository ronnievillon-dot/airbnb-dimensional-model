from src.pipeline.extract import extract_listings

df = extract_listings()

print(df.shape)
print(df.head())
