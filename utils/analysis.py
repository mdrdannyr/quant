import config, logging, os, traceback, time
from utils import log, database_builder, database_loader, database_reader, instruments_collector, instruments_data_collector

logger = logging.getLogger(config.logger_name)


class Analyse():

    def __init__(self):
        self.db = database_reader.ReadDatabase()


    def get_instruments(self):
        df = self.db.get_processed_instruments_ex_symbol_symbols_exchange()

        return df


    def get_stock(self, instrument_pk):
        df = self.db.get_processed_instruments_data_date_close(instrument_pk)

        return df