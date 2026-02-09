from src.pipeline.extract import extract_listings
from src.pipeline.validate import ejecutar_validaciones_listings
from src.pipeline.transform import transformar_listings
from src.pipeline.load import ejecutar_carga
from src.utils.logger import get_logger


logger = get_logger()


def run_pipeline():

    try:

        logger.info("🚀 Iniciando pipeline Airbnb")

        df = extract_listings()

        logger.info(f"Extract completado — filas: {len(df)}")

        df = ejecutar_validaciones_listings(df)

        logger.info("Validación completada")

        df = transformar_listings(df)

        logger.info("Transformación completada")

        ejecutar_carga(df)

        logger.info("Carga al DW completada")

        logger.info("✅ PIPELINE FINALIZADO CON ÉXITO")

    except Exception as e:

        logger.exception("🔥 ERROR EN PIPELINE")
        raise


if __name__ == "__main__":
    run_pipeline()
