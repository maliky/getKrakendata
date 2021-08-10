"""
Download trade data for a kraken asset pair. Updates can be downloaded by
simply calling this script again.

Data is stored as pandas.DataFrame's (in "unixtimestamp.csv" format).
Use pd.read_csv(file) to load data into memory.

Use the ``interval`` argument to sample trade data into ohlc format instead of
downloading/updating trade data. Data is stored as a pandas.DataFrame (in
"_olhc_interval.csv" format).
"""

import argparse
import os
from pathlib import Path
from pprint import pformat
import pytz
from time import sleep

import pandas as pd
import getKrakenData.getKrakenData as kapi
from mlkHelper.timeUtils import ts_extent
import logging

logger = logging.getLogger()


class GetTradeData(object):
    def __init__(
        self, folder, pair: str, timezone: str = "Africa/Abidjan", wait_time=1.2
    ):
        """
        folder : base folder name
        pair: name of the pair to download
        timezone: in the forme Africa/Abidjan
        wait_time: time to wait between call (default 1.2 s)
        """
        # initiate api
        self.kapi = kapi.KolaKrakenAPI(tier=None, retry=2, crl_sleep=5)

        # set pair
        self.pair = pair
        self.tz = pytz.timezone(timezone)

        # set and create folder
        self.folder = folder
        self.folder = Path(f"{folder}/{pair}")
        os.makedirs(self.folder, exist_ok=True)

        self.wait_time = wait_time

    def __repr__(self):
        _repr = {
            "kapi": self.kapi,
            "pair": self.pair,
            "tz": self.tz,
            "folder": self.folder,
            "wait_time": self.wait_time,
        }
        return pformat(_repr)

    def download_trade_data(self, since, end_ts):

        # update or new download?
        if not since:
            fs = [f for f in os.listdir(self.folder) if not f.startswith("_")]

            # get the last time stamp in the folder to run an update
            if len(fs) > 0:
                fs.sort()
                next_start_ts = int(fs[-1].split(".")[0])
            else:
                next_start_ts = 0
        else:
            next_start_ts = since

        # get data
        while next_start_ts < end_ts.timestamp():

            trades = self.kapi.get_recent_trades(pair_=self.pair, since_=next_start_ts)
            if not len(trades):
                raise Exception(f"not trades : {self}")

            start_ts, next_start_ts = ts_extent(trades, as_unix_ts_=True)

            try:
                # set timezone
                index = trades.index.tz_localize(pytz.utc).tz_convert(self.tz)
                trades.index = index
            except AttributeError as ae:
                print(
                    f"### trades={trades}; next_start_ts={next_start_ts} ################"
                )
                raise (ae)

            # store
            fout = self.folder.joinpath(f"{start_ts}.csv")
            print(
                f"Trade data from ts {start_ts} ({pd.Timestamp(start_ts*1e9)}) --> {fout}"
            )
            trades.to_csv(fout)
            sleep(self.wait_time)

        print("\n download/update finished!")

    def agg_ohlc(self, since: int, interval: int = 1):

        # fetch files and convert to dataframe
        _fs = [
            self.folder.joinpath(f)
            for f in os.listdir(self.folder)
            if not f.startswith("_")
        ]
        _fs.sort(reverse=True)

        if since > 0:
            _fs = [f for f in _fs if int(f.name.split(".")[0]) >= since]

        _trades = [pd.read_csv(f) for f in _fs]

        trades = pd.concat(_trades, axis=0)
        trades.index = pd.to_datetime(trades.tsh)
        trades = trades.drop("tsh", axis=1)

        trades.loc[:, "cost"] = trades.price * trades.volume

        # resample
        gtrades = trades.resample(pd.Timedelta(f"{interval}min"))

        # ohlc, volume
        ohlc = gtrades.price.ohlc()
        ohlc.loc[:, "volume"] = gtrades.volume.sum()
        ohlc.volume.fillna(0, inplace=True)
        closes = ohlc.close.fillna(method="pad")
        ohlc = ohlc.apply(lambda x: x.fillna(closes))

        # vwap
        ohlc.loc[:, "vwap"] = gtrades.cost.sum() / ohlc.volume
        ohlc.vwap.fillna(ohlc.close, inplace=True)

        # count
        ohlc.loc[:, "nb"] = gtrades.size()

        start_tsh, end_tsh = ts_extent(ohlc, as_unix_ts_=False)
        start_ts, end_ts = ts_extent(ohlc, as_unix_ts_=True)
        # store on disc
        fout = self.folder.joinpath(f"_ohlc_{start_ts}-{end_ts}_{interval}m.csv")
        print(f"Storing OHLC from {start_tsh} to {end_tsh} --> {fout.name}")
        ohlc.to_csv(fout)
        return ohlc


def main(
    folder: str, pair: str, since: int, timezone: str, interval: int, waitTime: int
):

    dl = GetTradeData(folder, pair, timezone, waitTime)
    end_ts = pd.Timestamp.now() - pd.Timedelta("60s")

    logger.info(
        f"Starts downloading in {folder}/{pair} with TZ {timezone}. from {since}-to {end_ts}"
    )
    dl.download_trade_data(since, end_ts)

    if interval:
        logger.info(f"computing ohlc for {interval}m.")
        dl.agg_ohlc(since, interval)


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--folder",
        help=(
            "In which (parent) folder to store data.  The final folder "
            " is the asset pair name"
        ),
        type=str,
        default="./Kraken",
    )

    parser.add_argument(
        "--pair",
        help=(
            "asset pair for which to get the trade data. "
            "See KrakenAPI(api).get_tradable_asset_pairs().index.values"
        ),
        type=str,
        default="ADAXBT",
    )

    parser.add_argument(
        "--since",
        help=(
            "download/aggregate trade data since given unixtime (exclusive)."
            " If 0 and this script was called before, only an"
            " update to the most recent data is retrieved. If 0 and this"
            " function was not called before, retrieve from earliest time"
            " possible. When aggregating (interval>0), aggregate from"
            " ``since`` onwards (unixtime)."
        ),
        type=int,
        default=0,
    )

    parser.add_argument(
        "--timezone",
        help=(
            "convert the timezone of timestamps to ``timezone``, which must "
            "be a string that pytz.timezone() accepts (see "
            "pytz.all_timezones)"
        ),
        type=str,
        default="Africa/Abidjan",
    )

    parser.add_argument(
        "--interval",
        help=(
            "sample downloaded trade data to ohlc format with the given time"
            "interval (minutes). If 0 (default), only download/update trade "
            "data."
        ),
        type=int,
        default=0,
    )

    parser.add_argument(
        "--waitTime",
        help=("time to wait between calls in second"),
        type=int,
        default=1.2,
    )

    return parser.parse_args()


def main_prg():
    args = parse_args()
    # execute
    main(
        folder=args.folder,
        pair=args.pair,
        since=args.since,
        timezone=args.timezone,
        interval=args.interval,
        waitTime=args.waitTime,
    )


if __name__ == "__main__":
    main_prg()
