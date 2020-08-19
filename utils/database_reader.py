import config, logging, os, traceback
import sqlite3
import pandas

logger = logging.getLogger(config.logger_name)


class ReadDatabase():

    def __init__(self):

        if os.path.isfile(config.database_path):
            try:
                self.conn = sqlite3.connect(config.database_path)
            except:
                logger.error('ReadDatabase#__init__: Error connecting to Database')
                logger.debug(traceback.format_exc())
        else:
            logger.warning('ReadDatabase#__init__: Database file does not exist')

    def get_all_tables(self):

        df = pandas.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", self.conn)

        return df

    def get_all_instruments_ex_symbol_symbols_exchange(self):

        df = pandas.read_sql_query("SELECT ex_symbol, exchange, symbol FROM instruments", self.conn)

        return df

    def get_unprocessed_instruments_ex_symbol_symbols_exchange(self):

        df = pandas.read_sql_query("SELECT ex_symbol, exchange, symbol FROM instruments WHERE data_load_date is NULL", self.conn)

        return df

    def get_processed_instruments_ex_symbol_symbols_exchange(self):

        df = pandas.read_sql_query("SELECT ex_symbol, exchange, symbol FROM instruments WHERE data_load_date is NOT NULL", self.conn)

        return df

    def get_processed_instruments_data_date_close(self, instrument_pk):

        df = pandas.read_sql_query("SELECT date, close FROM instrumentdata WHERE instrument_pk = '{}'".format(instrument_pk), self.conn)

        return df

    def get_lse_instruments_symbols_exchange(self):

        df = pandas.read_sql_query("SELECT exchange, symbol FROM instruments WHERE exchange == 'LSE'", self.conn)

        return df

    def get_nasdaq_instruments_symbols_exchange(self):

        df = pandas.read_sql_query("SELECT exchange, symbol FROM instruments WHERE exchange == 'NASDAQ'", self.conn)

        return df

    def get_nyse_instruments_symbols_exchange(self):

        df = pandas.read_sql_query("SELECT exchange, symbol FROM instruments WHERE exchange == 'NYSE'", self.conn)

        return df

    def __del__(self):

        self.conn.close()
