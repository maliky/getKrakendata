# -*- coding: utf-8 -*-
from pandas import DataFrame
from pathlib import Path
from typing import Sequence


def load_kraken_ohlc_data(
    years: Sequence = [2019],
    bins: str = "1m",
    dirname=Path("/media/data/Crypto-data/Kraken"),
    pair="ADAUSD",
) -> DataFrame:
    """
    Load a sequence of ohlc file in one DataFrame
    """
    df = None
    for year in years:
        fname = f"{pair}-{bins}-{year}.csv"
        _tmp = pd.read_csv(dirname.joinpath(fname))
        df = _tmp if df is None else pd.concat([df, _tmp], axis=0)

    df.loc[:, "tsh"] = pd.to_datetime(df.tsh)
    df = df.set_index("tsh")
    df = df.sort_index()
    return  df
