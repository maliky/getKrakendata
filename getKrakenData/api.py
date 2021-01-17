# -*- coding: utf-8 -*-
"""function to simplifie kraken api calls"""
from typing import List
import functools

from numpy import dtype
from pandas import Series, DataFrame, Timestamp
import pandas as pd


import krakenex as krx
from pykrakenapi.pykrakenapi import crl_sleep, callratelimiter

# https://www.kraken.com/features/api#public-market-data

# set number of 0 after the ts, should be linke do the time resolution
# pd.set_option("display.float_format", lambda x: f"{x:0<9.0f}")


def fmt_output(as_="DataFrame"):
    """Décore les API de kraken to get dataFrame outputs"""

    def wrapper(f):
        @functools.wraps(f)
        def wrapped_f(*args, **kwargs):
            _res = f(*args, **kwargs)
            _res = _res.get("result", {"error": _res.get("error")})
            if as_ == "DataFrame":
                return DataFrame(_res)
            elif as_ == "Series":
                return Series(_res)
            return _res

        return wrapped_f

    return wrapper


class KolaKrakenAPI:
    def __init__(self, keyfile="keys.txt", tier="Intermediate", retry=1, crl_sleep=5):
        """Init for KolaKraken"""
        self._api = krx.API()
        self._api.load_key("keys.txt")
        self._asset = None
        self._asset_pairs = None

        # api call rate limiter
        self.time_of_last_public_query = None
        self.time_of_last_query = pd.Timestamp.now()

        self.api_counter = 0

        if tier == "None":
            self.limit = float("inf")
            self.factor = 3  # does not matter

        elif tier == "Starter":
            self.limit = 15
            self.factor = 3  # down by 1 every three seconds

        elif tier == "Intermediate":
            self.limit = 20
            self.factor = 2  # down by 1 every two seconds

        elif tier == "Pro":
            self.limit = 20
            self.factor = 1  # down by 1 every one second

        # retry timers
        self.retry = retry
        self.crl_sleep = crl_sleep

    @fmt_output()
    def get_assets(self):
        self._asset = self._api.query_public("Assets")
        return self._asset

    @fmt_output(as_="Series")
    def get_time(self):
        return self._api.query_public("Time")

    @fmt_output(as_="Series")
    def status(self):
        return self._api.query_public("SystemStatus")

    @fmt_output()
    def get_asset_pairs(self):
        """
        base_ est la monnaie d'ont on affiche le prix en quote_
        Renvois les pairs qui peuvent être échangé
        """
        self._asset_pairs = (
            self._api.query_public("AssetPairs")
            if self._asset_pairs is None
            else self._asset_pairs
        )
        return self._asset_pairs

    def get_tradable_pairs(self, base_="", quote_=""):
        """
        Renvois les pairs qui peuvent être échangéé
        base_ est la monnaie d'ont on affiche le prix en quote_
        """
        _asset_pairs = self.get_asset_pairs()
        return [
            c
            for c in _asset_pairs.columns
            if c.startswith(base_) and c.endswith(quote_)
        ]

    @fmt_output()
    def get_ticker_info(self, pairs: List = ["ADAXBT"]):
        """
        Get l'information du ticket
        a = ask array(<price>, <whole lot volume>, <lot volume>),
        b = bid array(<price>, <whole lot volume>, <lot volume>),
        c = last trade closed array(<price>, <lot volume>),
        v = volume array(<today>, <last 24 hours>),
        p = volume weighted average price array(<today>, <last 24 hours>),
        t = number of trades array(<today>, <last 24 hours>),
        l = low array(<today>, <last 24 hours>),
        h = high array(<today>, <last 24 hours>),
        o = today's opening price
        """
        return self._api.query_public("Ticker", data={"pair": pairs})

    @fmt_output(as_="raw")
    def get_ohlc_data(self, pair_="ADAXBT", interval_=1, since_=None):
        """
        pair = asset pair to get OHLC data for
        interval = time frame interval in minutes (optional):
            1 (default), 5, 15, 30, 60, 240, 1440, 10080, 21600
        since = return committed OHLC data since given id (optional.  exclusive)

        Return : <pair_name> = pair name
        array of array entries(<time>, <open>, <high>, <low>, <close>, <vwap>, <volume>, <count>)
        last = id to be used as since when polling for new, committed OHLC data
        """

        return self._api.query_public(
            "OHLC", data={"pair": pair_, "interval": interval_, "since": since_}
        )

    def get_ohlc(self, pair_="ADAXBT", interval_=1, since_=None):
        """
        pair = asset pair to get OHLC data for
        interval = time frame interval in minutes (optional):
            1 (default), 5, 15, 30, 60, 240, 1440, 10080, 21600
        since = return committed OHLC data since given id (optional.  exclusive)

        Return : <pair_name> = pair name
        array of array entries(<time>, <open>, <high>, <low>, <close>, <vwap>, <volume>, <count>)
        last = id to be used as since when polling for new, committed OHLC data
        """
        _data = self.get_ohlc_data(pair_, interval_, since_)[pair_]
        ohlc = DataFrame(
            data=_data, columns=["ts", "o", "h", "l", "c", "vwap", "volume", "count"],
        )
        ohlc = make_tsh_index(ohlc)
        return ohlc

    @crl_sleep
    @callratelimiter("public")
    @fmt_output(as_="raw")
    def get_recent_trades_raw(self, pair_="ADAXBT", since_=None):
        """
        pair = asset pair to get trade data for
        since = return trade data since given id (optional.  exclusive)

        Return <pair_name> = pair name
        array of array entries(<price>, <volume>, <time>, <buy/sell>, <market/limit>, <miscellaneous>)
        last = id to be used as since when polling for new trade data
        """

        return self._api.query_public("Trades", data={"pair": pair_, "since": since_})

    def get_recent_trades(self, pair_="ADAXBT", since_=None, drop_ts_: bool = True):
        """
        array of array entries(<price>, <volume>, <time>, <buy/sell>, <market/limit>, <miscellaneous>)
        last = id to be used as since when polling for new trade data
        drop_ts_ should the ts columns be droped?
        """
        raw_trades = self.get_recent_trades_raw(pair_, since_)
        trades = DataFrame(
            data=raw_trades[pair_],
            columns=["price", "volume", "ts", "side", "order", "misc"],
        )
        trades = make_tsh_index(trades, drop_=drop_ts_)
        return trades

    @fmt_output(as_="raw")
    def get_order_book_raw(self, pair_="ADAXBT", count_=None):
        """
        pair = asset pair to get market depth for
        count = maximum number of asks/bids (optional)
        return:
        <pair_name> = pair name
        asks = ask side array of array entries(<price>, <volume>, <timestamp>)
        bids = bid side array of array entries(<price>, <volume>, <timestamp>)        """
        return self._api.query_public("Depth", data={"pair": pair_, "count": count_})

    def get_order_book(self, pair_="ADAXBT", count_=None):
        """
        pair = asset pair to get market depth for
        count = maximum number of asks/bids (optional)
        return:
        <pair_name> = pair name
        asks = ask side array of array entries(<price>, <volume>, <timestamp>)
        bids = bid side array of array entries(<price>, <volume>, <timestamp>)        """
        _datum = ["price", "volume", "ts"]

        _order_book = self.get_order_book_raw(pair_, count_)[pair_]
        df = None
        for side in ["asks", "bids"]:
            MC = pd.MultiIndex.from_product([[side], _datum])
            _tmp = DataFrame(_order_book[side], columns=MC)
            _tmp.columns.name = side
            df = _tmp if df is None else pd.concat([df, _tmp], axis=1)

        # before changing the index, I should make sure that both asks and bids have same ts
        # df = make_tsh_index(df, col_to_tsh_=("asks", "ts"), drop_=False)
        # df = df.swaplevel(axis=1).drop("ts", axis=1, level=0).swaplevel(axis=1)
        return df

    @fmt_output(as_="DataFrame")
    def get_recent_spread_data(self, pair_="ADAXBT", since_=None):
        """
        pair = asset pair to get spread data for
        since = return spread data since given id (optional.  inclusive)
        
        return:
        <pair_name> = pair name
        array of array entries(<time>, <bid>, <ask>)
        last = id to be used as since when polling for new spread data
        """

        return self._api.query_public("Spread", data={"pair": pair_, "since": since_})


def to_tsh(ts_serie, tsh_resolution_="ms", ts_factor_=1e9):
    """
    Timestamp human (tsh) readable 
    Convert a series of ts from pd.Timestamp().timestamp to a readable date
    
    tsh_resolution_ (def s) : how should the ts be rounded?
    ts_factor_ is the factor by which to multiply the ts_serie. before convertion.
    1 for ms resolution, 1e9 for nano resolution
    """
    if ts_serie.dtype == dtype("O"):
        # suppos it is str and in human readable format
        return pd.to_datetime(ts_serie)

    return list(
        map(lambda ts: ts.round(tsh_resolution_), pd.to_datetime(ts_serie * ts_factor_))
    )


def make_tsh_index(df_: DataFrame, col_to_tsh_: str = "ts", drop_: bool = True):
    """
    make the index fo the dataframe a timestamp index using the col_to_tsh_

    drop_ true to drop the col_to_tsh after transformation
    """
    # import ipdb; ipdb.set_trace()

    df_.index = to_tsh(df_.loc[:, col_to_tsh_])

    df_.index.name = "tsh"
    df_ = df_.drop(col_to_tsh_, axis=1) if drop_ else df_

    return df_


def get_ts_from_tsh(tsh_, ts_round_=0):
    """
    Converti un timestamp en unix format
    """
    return int(tsh_.timestamp())
