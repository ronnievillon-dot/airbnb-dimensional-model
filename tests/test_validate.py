from src.pipeline.extract import extract_listings
from src.pipeline.validate import ejecutar_validaciones_listings

df = extract_listings()

df_limpio = ejecutar_validaciones_listings(df)

print("\nShape del dataset limpio:")
print(df_limpio.shape)
