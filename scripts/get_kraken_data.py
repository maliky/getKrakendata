"""
Download trade data for a kraken asset pair. Updates can be downloaded by
simply calling this script again.

Data is stored as pandas.DataFrame's (in "unixtimestamp.pickle" format).
Use pd.read_pickle(file) to load data into memory.

Use the ``interval`` argument to sample trade data into ohlc format instead of
downloading/updating trade data. Data is stored as a pandas.DataFrame (in
"pair_interval.pickle" format).

"""

import argparse
import os
from pathlib import Path
import pytz
from time import sleep

import pandas as pd
import getKrakenData.getKrakenData as kapi
from btxAnalysis import stattimes as st


class GetTradeData(object):
    def __init__(self, bname, pair: str, timezone: str = "Africa/Abidjan", wait_time=1):
        """
        bname : base folder name
        pair: name of the pair to download
        timezone: in the forme Africa/Abidjan
        wait_time: time to wait between call (default 1)
        """
        # initiate api
        self.k = kapi.KolaKrakenAPI(tier=None, retry=0.1)

        # set pair
        self.pair = pair
        self.tz = pytz.timezone(timezone)

        # set and create folder
        self.bname = bname
        self.folder = Path(f"{bname}-{pair}")
        os.makedirs(self.folder, exist_ok=True)

        self.wait_time = wait_time

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

            trades = self.k.get_recent_trades(pair_=self.pair, since_=next_start_ts)
            start_ts, next_start_ts = st.ts_extent(trades, as_unix_ts_=True)

            # set timezone
            index = trades.index.tz_localize(pytz.utc).tz_convert(self.tz)
            trades.index = index

            # store
            fout = self.folder.joinpath(f"{start_ts}.csv")
            print(
                f"Trade data from ts {start_ts} ({pd.Timestamp(start_ts*1e9)}) --> {fout}"
            )
            trades.to_csv(fout)
            sleep(self.wait_time)

        print("\n download/update finished!")

    def agg_ohlc(self, since: int, interval:int=1):

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
        ohlc.loc[:, "count"] = gtrades.size()

        start_tsh, end_tsh = st.ts_extent(ohlc, as_unix_ts_=False)
        start_ts, end_ts = st.ts_extent(ohlc, as_unix_ts_=True)
        # store on disc
        fout = self.folder.joinpath(f"_ohlc_{start_ts}-{end_ts}_{interval}m.csv")
        print(f"Storing OHLC from {start_tsh} to {end_tsh} --> {fout.name}")
        ohlc.to_csv(fout)


def main(
    bname: str, pair: str, since: int, timezone: str, interval: str, waitTime: int
):

    dl = GetTradeData(bname, pair, timezone, waitTime)
    end_ts = pd.Timestamp.now() - pd.Timedelta("60s")

    dl.download_trade_data(since, end_ts)

    if interval:
        dl.agg_ohlc(since, interval)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--bname",
        help="which (parent) folder to store data in",
        type=str,
        default="./cryptodata",
    )

    parser.add_argument(
        "--pair",
        help=(
            "asset pair to get trade data for. "
            "see KrakenAPI(api).get_tradable_asset_pairs().index.values"
        ),
        type=str,
        # default="XXBTZEUR",
        default="ADAXBT",
    )

    parser.add_argument(
        "--since",
        help=(
            "download/aggregate trade data since given unixtime (exclusive)."
            " If 0 (default) and this script was called before, only an"
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

    args = parser.parse_args()

    # execute
    main(
        bname=args.bname,
        pair=args.pair,
        since=args.since,
        timezone=args.timezone,
        interval=args.interval,
        waitTime=args.waitTime,
    )
