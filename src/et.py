#! /usr/bin/env python
# Copyright 2024 John Hanley. MIT licensed.
"""Does initial ETL steps: extracts and transforms IMDB data into fast parquet files."""
import re
from collections.abc import Callable
from pathlib import Path

import pandas as pd

DATA_DIR = Path("data").resolve()


def get_actor() -> pd.DataFrame:
    """Returns a DataFrame of Internet Movie DataBase actor name details."""
    return _read_parquet(DATA_DIR / "name.basics.tsv.gz", _xform_actor)


def _xform_actor(df: pd.DataFrame) -> pd.DataFrame:
    """Transforms / cleans up actor details."""
    pattern = re.compile("actor|actress")
    df = df.dropna()
    df = df[df.primaryProfession.str.contains(pattern)]  # type: ignore
    df = df.drop(columns=["primaryProfession"])
    df = df[pd.to_numeric(df.birthYear, errors="coerce") >= 1899]
    df["birthYear"] = pd.to_numeric(df.birthYear)
    df["deathYear"] = pd.to_numeric(df.deathYear, errors="coerce")
    df = df.rename(
        columns={
            "nconst": "id",
            "primaryName": "name",
            "birthYear": "born",
            "deathYear": "died",
            "knownForTitles": "titles",
        }
    )
    return df


def get_movie() -> pd.DataFrame:
    """Returns a DataFrame of IMDB movie title details."""
    return _read_parquet(DATA_DIR / "title.basics.tsv.gz", _xform_movie)


def _xform_movie(df: pd.DataFrame) -> pd.DataFrame:
    """Transforms / cleans up title details."""
    df = df.dropna()
    df = df[df.titleType == "movie"]
    df = df[df.isAdult == "0"]
    df = df.drop(columns=["titleType", "originalTitle", "isAdult", "endYear"])
    df = df[df.genres.str.len() > 2]
    df = df[pd.to_numeric(df.startYear, errors="coerce") > 1990]
    df = df[pd.to_numeric(df.runtimeMinutes, errors="coerce") > 60]
    df["startYear"] = pd.to_numeric(df.startYear)
    df["runtimeMinutes"] = pd.to_numeric(df.runtimeMinutes)
    df = df.rename(
        columns={
            "tconst": "id",
            "primaryTitle": "title",
            "startYear": "year",
            "runtimeMinutes": "minutes",
        }
    )
    return df


def _read_parquet(
    in_file: Path,
    xform: Callable[[pd.DataFrame], pd.DataFrame],
) -> pd.DataFrame:
    """Reads the requested parquet-format file.

    In the event of a cache miss, we read the downloaded TSV,
    use the supplied dataframe transform routine to clean up columns
    and discard uninteresting rows,
    then finally cache the results.
    """
    name = in_file.name.removesuffix(".tsv.gz")
    cache_file = DATA_DIR / f"{name}.parquet"
    if not cache_file.exists():
        print(f"Reading {in_file} ...")
        df = pd.read_csv(in_file, sep="\t", low_memory=False)

        print(f"Transforming {in_file} ...")
        df = xform(df)

        print(f"Creating {cache_file} ...")
        df.to_parquet(cache_file)
        print(".")

    return pd.read_parquet(cache_file)


def main() -> None:
    print(actor := get_actor())
    print(movie := get_movie())
    print(actor.dtypes)
    print(movie.dtypes)


if __name__ == "__main__":
    main()
