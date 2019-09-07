""" Options Strangle Builder """
from sqlalchemy import create_engine, Table, MetaData, and_
from sqlalchemy.sql import select
from datetime import timedelta, datetime
import pandas as pd
import numpy as np


class opb(object):
    """ Options strangel builder class"""

    def __init__(self):
        self.db = create_engine("sqlite:///data.db")
        self.meta_data = MetaData(self.db)
        self.table = Table("file", self.meta_data, autoload=True)
        self.strategy = None

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
        """
        raise NotImplementedError("To be implemented")

    def create_strangle_position(self, cs, ps, csp=None, psp=None, at=None):
        """
        Creates strangle position
        -------------------------------------------------
        Parameters
        ----------
        cs -> (int) Call strike
        ps -> (int) Put strike
        csp -> (float) Call strike price, if not given takes latest price
        psp -> (float) Put strike price, if not given takes latest price
        at -> (int) Time, if not given takes latest data available
        """
        if csp is not None:
            raise NotImplementedError("To be implemented")
        if psp is not None:
            print(f"PSP = {psp}")
            raise NotImplementedError("To be implemented")
        if at is not None:
            raise NotImplementedError("To be implemented")

        csdf = self.get_strikes_latest_data(cs, "CE")
        psdf = self.get_strikes_latest_data(ps, "PE")
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