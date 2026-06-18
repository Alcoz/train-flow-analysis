import polars as pl


def txt_to_parquet_polars(
    input_txt_path: str,
    output_parquet_path: str,
    separator: str = ",",
    has_header: bool = True,
) -> None:
    """
    Convertit un fichier TXT tabulaire en Parquet via Polars.

    Parameters
    ----------
    input_txt_path : str
        Chemin du fichier source .txt
    output_parquet_path : str
        Chemin du fichier .parquet de sortie
    separator : str
        Délimiteur (',' , '\\t', ';', etc.)
    has_header : bool
        Indique si la première ligne contient les noms de colonnes
    """

    df = pl.read_csv(
        input_txt_path,
        separator=separator,
        has_header=has_header,
        infer_schema_length=10_000,  # améliore l'inférence de types
        ignore_errors=True,  # utile si données légèrement sales
    )

    df.write_parquet(output_parquet_path)
