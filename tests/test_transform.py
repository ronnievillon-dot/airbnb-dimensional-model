from src.pipeline.extract import extract_listings
from src.pipeline.validate import ejecutar_validaciones_listings
from src.pipeline.transform import transformar_listings

df = extract_listings()
df = ejecutar_validaciones_listings(df)

df_transformado = transformar_listings(df)

print("\nColumnas nuevas:")
print(df_transformado.columns)

print("\nShape final:")
print(df_transformado.shape)
