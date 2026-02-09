import pandas as pd
from pathlib import Path


def get_data_path(filename: str) -> Path:
    """
    Crea una ruta confiable a la carpeta de datos. 
    Funciona independientemente de dónde se ejecute el script.
    """

    project_root = Path(__file__).resolve().parents[2]
    data_path = project_root / "data" / filename

    return data_path


def extract_listings():
    """
    Extrae listings dataset.
    """

    path = get_data_path("listings.csv")

    df = pd.read_csv(
        path,
        encoding="utf-8",
        low_memory=False
    )

    return df


def extract_reviews():
    """
    Extrae reviews dataset.
    """

    path = get_data_path("reviews.csv")

    df = pd.read_csv(
        path,
        encoding="utf-8",
        low_memory=False
    )

    return df
