import config, logging, os, traceback
from utils import database_reader
from alpha_vantage.timeseries import TimeSeries

logger = logging.getLogger(config.logger_name)


class AlphaVantage():

    def __init__(self):

        # Create Timeseries ts using AV
        self.ts = TimeSeries(config.ALPHA_VANTAGE_key, output_format='pandas')

    def create_new_timeseries(self, apikey):

        # Create Timeseries ts using AV
        self.ts = TimeSeries(apikey, output_format='pandas')

    def get_lse_instrument_data(self, symbol):

        symbol = 'LON:' + symbol

        df, meta = self.ts.get_daily(symbol=symbol, outputsize='full')

        return df

    def get_nasdaq_instrument_data(self, symbol):

        symbol = 'LON:' + symbol

        df, meta = self.ts.get_daily(symbol=symbol, outputsize='full')

        return df

    def get_nyse_instrument_data(self, symbol):

        symbol = 'LON:' + symbol

        df, meta = self.ts.get_daily(symbol=symbol, outputsize='full')

        return df

    def get_instrument_data(self, exchange, symbol):
        try:

            while "." in symbol or "$" in symbol:
                if symbol[-1] == ".":
                    symbol = symbol[:-1]
                if "." in symbol:
                    symbol = symbol.replace(".", "-")
                if "$" in symbol:
                    symbol = symbol.replace("$", "-P")

            if exchange == 'LSE':
                symbol = 'LON:' + symbol
            if exchange == 'NYSE':
                symbol = symbol
            if exchange == 'NASDAQ':
                pass

            df, meta = self.ts.get_daily(symbol=symbol, outputsize='full')

            res = {}
            res['df'] = df
            res['error'] = False

            return res

        except ValueError as err:
            logger.error("value error: \n exchange={}, \n symbol={}, \n error message ={}"
                         .format(str(exchange), str(symbol), str(err)))

            res = {}
            res['error'] = str(err)

            if "Invalid API call" in str(err):
                res['error'] = "Invalid API call"

            if "Our standard API call frequency is 5 calls per minute and 500 calls per day" in str(err):
                res['error'] = "API 5/min or 500/day Limit Reached"

            return res




