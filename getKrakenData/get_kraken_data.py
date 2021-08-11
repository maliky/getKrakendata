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
from typing import Sequence, List
from time import sleep

from pandas import read_csv, concat, Timedelta, Timestamp, to_datetime
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
        self.folder = Path(folder)
        self.folder_data = Path(folder).joinpath(pair)
        os.makedirs(self.folder_data, exist_ok=True)

        self.wait_time = wait_time

    def __repr__(self):
        _repr = {
            "kapi": self.kapi,
            "pair": self.pair,
            "tz": self.tz,
            "folder": self.folder,
            "folder_data": self.folder_data,
            "wait_time": self.wait_time,
        }
        return pformat(_repr)

    def download_trade_data(self, since, end_ts):

        # update or new download?
        if not since:
            fs = [f for f in os.listdir(self.folder_data) if not f.startswith("_")]

            # get the last time stamp in the folder_data to run an update
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
                    f"###:{slef}: trades={trades}, {type(trades)}; next_start_ts={next_start_ts} ####"
                )
                raise (ae)

            # store
            fout = self.folder_data.joinpath(f"{start_ts}.csv")
            print(
                f"Trade data from ts {start_ts} ({Timestamp(start_ts*1e9)}) --> {fout}"
            )
            trades.to_csv(fout)
            sleep(self.wait_time)

        print("\n download/update finished!")

    def get_data_files(self, since: int = 0) -> List[Path]:
        """
        Returns a list of data file from the data_folder
        since is the timestamp (unix format) from which to get the data files
        """
        _fs = [
            self.folder_data.joinpath(f)
            for f in os.listdir(self.folder_data)
            if not f.startswith("_")
        ]
        _fs.sort(reverse=True)

        if since > 0:
            _fs = [f for f in _fs if int(f.name.split(".")[0]) >= since]

        return _fs

    def agg_ohlc(self, data_files: Sequence, interval: int = 1):
        """build the ohlc file"""
        # fetch files and convert to dataframe

        _trades = [read_csv(f) for f in data_files]

        trades = concat(_trades, axis=0)
        trades.index = to_datetime(trades.tsh)
        trades = trades.drop("tsh", axis=1)

        trades.loc[:, "cost"] = trades.price * trades.volume

        # resample
        gtrades = trades.resample(Timedelta(f"{interval}min"))

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
        return ohlc

    def save_ohlc(self, ohlc, interval, yearly_format: bool = False):
        """
        Saving a ohlc file to disque with start and end dates
        save_format
        """
        start_tsh, end_tsh = ts_extent(ohlc, as_unix_ts_=False)
        start_ts, end_ts = ts_extent(ohlc, as_unix_ts_=True)
        # store on disc
        if yearly_format:
            fout = self.folder.joinpath("{self.pair}-{interval}m-{end_tsh.year}.csv")
        else:
            fout = self.folder.joinpath(f"_ohlc_{start_ts}-{end_ts}_{interval}m.csv")

        print(f"Storing OHLC from {start_tsh} to {end_tsh} --> {fout.name}")
        ohlc.to_csv(fout)

    def agg_ohlc_yearly(self, years: List[int] = [], interval: int = 1):
        """
        aggregate data from the folder_data in a yearly ohlc dataframe
        year should be a sequence of years.  If none make one big ohlc dataframe
        """

        def _get_yearly_data_file(year):
            """Returns the bunch of file corresponding to year"""
            set_a = set(self.get_data_files(Timestamp(f"{year}").timestamp()))
            set_b = set(self.get_data_files(Timestamp(f"{year+1}").timestamp()))
            return list(set_a - set_b)

        if len(years) == 0:
            now_year = Timestamp.now().year()
            years = list(range(now_year, now_year - 10))

        for year in years:
            _ohlc = self.agg_ohlc(_get_yearly_data_file(year), interval)
            self.save_ohlc(_ohlc, interval, yearly_format=True)


def main(
    folder: str,
    pair: str,
    since: int,
    timezone: str,
    interval: int,
    waitTime: int,
    years: bool,
):

    dl = GetTradeData(folder, pair, timezone, waitTime)
    end_ts = Timestamp.now() - Timedelta("60s")

    logger.info(
        f"Starts downloading in {folder}/{pair} with TZ {timezone}. from {since}-to {end_ts}"
    )
    dl.download_trade_data(since, end_ts)

    if interval:
        logger.info(f"Computing ohlc for {interval}m.")
        if not len(years):
            _data_files = dl.get_data_files(since)
            _ohlc = dl.agg_ohlc(_data_files, interval)
            logger.info("Saving...")
            dl.save_ohlc(_ohlc, interval)
        else:
            self.agg_ohlc_yearly(years, interval)


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--folder",
        "-f",
        help=(
            "In which folder store the data?  The folder_data containing data file "
            " will be a subfolder with the asset pair name"
        ),
        type=str,
        default="./Kraken",
    )

    parser.add_argument(
        "--pair",
        "-p",
        help=(
            "asset pair for which to get the trade data. "
            "See KrakenAPI(api).get_tradable_asset_pairs().index.values"
        ),
        type=str,
        default="ADAXBT",
    )

    parser.add_argument(
        "--since",
        "-s",
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
        "-t",
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
        "-i",
        help=(
            "sample downloaded trade data to ohlc format with the given time"
            "interval (minutes). If 0 (default), only download/update trade "
            "data."
        ),
        type=int,
        default=0,
    )

    parser.add_argument(
        "--years",
        "-Y",
        help=("if sampling downloaded trade data to ohlc set the years to sample"),
        type=list,
        default=[2021],
    )

    parser.add_argument(
        "--waitTime",
        "-w",
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
        years=args.years,
    )


if __name__ == "__main__":
    main_prg()
