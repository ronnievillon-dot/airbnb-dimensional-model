from src.pipeline.extract import extract_listings


def test_extract_not_empty():

    df = extract_listings()

    assert len(df) > 0
