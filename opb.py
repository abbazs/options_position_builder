""" Options Strangle Builder """
from sqlalchemy import create_engine, Table, MetaData, and_
from sqlalchemy.sql import select
from datetime import timedelta, datetime
import pandas as pd
import numpy as np


class opb(object):
    """ Options strangel builder class"""

    def __init__(self):
        self.db = create_engine("sqlite:///data1.db")
        self.meta_data = MetaData(self.db)
        self.table = Table("DATA", self.meta_data, autoload=True)
        self.strategy = None

    def push_to_db(self, data_in_dict):
        """
        Pushes data in dict to sqlite db
        -------------------------------------------------
        Parameters
        ----------
        data_in_dict -> (dict) instrument data
        -------------------------------------------------
        Returns
        -------
        None
        """
        if data_in_dict is None:
            raise Exception("No input data to push.")

        df = pd.DataFrame.from_dict(data_in_dict, orient="index")
        df = df.T
        df.to_sql("DATA", self.db, if_exists="append")

    def get_strikes_latest_data(self, strike, opt):
        """
        Get the strikes latest record available in the db
        -------------------------------------------------
        Parameters
        ----------
        strike -> (int) strike price
        opt -> (str) option type - CE or PE
        -------------------------------------------------
        Returns
        -------
        return -> (pandas data frame) having one row of data
        """
        dts = self.table
        stm = select(["*"]).where(and_(dts.c.strike == strike, dts.c.type == opt))
        sdf = pd.read_sql_query(stm, con=self.db, parse_dates=["exchange_timestamp"])
        sdf = sdf.sort_values("exchange_timestamp")
        if len(sdf) > 0:
            return sdf.tail(1)
        else:
            raise Exception("No data available for given inputs.")

    def get_strikes_data_at(self, strike, opt, time):
        """
        Get the strikes record at a given time
        -------------------------------------------------
        Parameters
        ----------
        strike -> (int) strike price
        opt -> (str) option type - CE or PE
        time -> (int) time in int format
        -------------------------------------------------
        Returns
        -------
        return -> (pandas data frame) having one row of data
        """
        dts = self.table
        stm = select(["*"]).where(
            and_(
                dts.c.strike == strike,
                dts.c.type == opt,
                dts.c.exchange_timestamp >= time,
            )
        )
        sdf = pd.read_sql_query(stm, con=self.db, parse_dates=["exchange_timestamp"])
        sdf = sdf.sort_values("exchange_timestamp")
        if len(sdf) > 0:
            return sdf.head(1)
        else:
            raise Exception("No data available for given inputs.")

    def create_strangle_position(self, cs, ps):
        """
        Creates strangle position
        -------------------------------------------------
        Parameters
        ----------
        cs -> (int) Call strike
        ps -> (int) Put strike
        """
        csdf = self.get_strikes_latest_data(cs, "CE")
        psdf = self.get_strikes_latest_data(ps, "PE")
        posdf = self.build_strangle_position(csdf, psdf)
        return posdf

    def create_strangle_position_at_time(self, cs, ps, at):
        """
        Creates strangle position
        -------------------------------------------------
        Parameters
        ----------
        cs -> (int) Call strike
        ps -> (int) Put strike
        at -> (int) Time
        """
        csdf = self.get_strikes_data_at(cs, "CE", at)
        psdf = self.get_strikes_data_at(ps, "PE", at)
        posdf = self.build_strangle_position(csdf, psdf)
        return posdf

    def build_strangle_position(self, csdf, psdf):
        """
        Builds strangle position from given data frames
        -------------------------------------------------
        Parameters
        ----------
        csdf -> (pandas.DataFrame) Call DataFrame
        psdf -> (pandas.DataFrame) Put DataFrame
        -------------------------------------------------
        Returns
        -------
        return -> (pandas data frame) having one row of data
        """
        csd = csdf[["strike", "last_traded_price", "exchange_timestamp"]]
        csd = csd.rename(
            columns={
                "strike": "CALL_STRIKE",
                "last_traded_price": "CALL_LTP",
                "exchange_timestamp": "CALL_ET",
            }
        )
        psd = psdf[["strike", "last_traded_price", "exchange_timestamp"]]
        psd = psd.rename(
            columns={
                "strike": "PUT_STRIKE",
                "last_traded_price": "PUT_LTP",
                "exchange_timestamp": "PUT_ET",
            }
        )
        posdf = csd.join(psd)
        posdf = posdf.assign(PNL=(posdf.CALL_LTP + posdf.PUT_LTP))
        self.strategy = posdf
        posdf.to_json("strategy.json")
        return posdf
