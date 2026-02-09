from src.pipeline.extract import extract_listings
from src.pipeline.validate import ejecutar_validaciones_listings
from src.pipeline.transform import transformar_listings
from src.pipeline.load import ejecutar_carga

df = extract_listings()
df = ejecutar_validaciones_listings(df)
df = transformar_listings(df)
print(df.dtypes)

ejecutar_carga(df)
