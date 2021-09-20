# -*- coding: utf-8 -*-
from pandas import DataFrame, read_csv, to_datetime, concat, date_range, to_numeric
from pathlib import Path
from typing import Sequence
from mlkHelper.timeUtils import timedelta_to_seconds
import logging


def load_kraken_ohlc_data(
    years: Sequence = [2019],
    bins: str = "1m",
    dirname=None,
    pair="ADAUSD",
) -> DataFrame:
    """
    Load a sequence of ohlc file in one DataFrame
    """
    dirname = (
        Path("/media/data/Crypto-data/Kraken") if dirname is None else Path(dirname)
    )
    df = None
    try:
        for year in years:
            fname = dirname.joinpath(f"{pair}-{bins}-{year}.csv")
            _tmp = read_csv(fname)
            df = _tmp if df is None else concat([df, _tmp], axis=0)
    except FileNotFoundError as fnfe:
        logging.exception(f"Check {fname}, {dirname}")
        raise (fnfe)

    # Création d'un Timesstamp index  à partir de la colonne des temps
    df.loc[:, "tsh"] = to_datetime(df.tsh)
    df = df.set_index("tsh")
    df = df.sort_index()
    df = df.drop_duplicates()

    # creating a full index for the requested bins
    fullIndex = date_range(
        start=df.index[0], end=df.index[-1], freq=timedelta_to_seconds(bins)
    )

    # creating a dataframe with this index
    fdf = DataFrame(index=fullIndex)
    fdf.loc[:, df.columns] = None
    # copying in it the data
    fdf.loc[df.index, :] = df

    # we fill the nans with previsous value in the nans
    fdf = fdf.fillna(method="ffill")

    # finaly we compute 2 more columns
    fdf.loc[:, "avg"] = (fdf.low + fdf.high) / 2
    fdf.loc[:, "amplitude"] = fdf.high - fdf.low

    # giving a name to the index
    fdf.index.name = "tsh"
    fdf.columns.name = "krk"

    return fdf
