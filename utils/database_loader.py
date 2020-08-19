import config, logging, os, traceback, datetime
import sqlite3
import utils.instruments_file_parser as ifp


logger = logging.getLogger(config.logger_name)


def load_lse_instruments_from_file():

    lse = ifp.LondonStockExchangeFileParser()
    df = lse.get_database_data()

    if os.path.isfile(config.database_path):
        try:
            conn = sqlite3.connect(config.database_path)
            # Create Temporary SQL table to hold up-to-date LSE Instruments
            df.to_sql(name='instrumentstemplse', con=conn, schema=None, if_exists='replace',
                      index=False, index_label=None, chunksize=None, dtype=None, method=None)
            conn.commit()
            conn.close()

        except:
            logger.error('load_lse_instruments: Error connecting to Database')
            logger.debug(traceback.format_exc())
    else:
        logger.warning('load_lse_instruments: Database file does not exist')


def update_lse_instruments_from_temp():

    if os.path.isfile(config.database_path):
        try:
            conn = sqlite3.connect(config.database_path)
            # INSERT any new instruments from the instrumenttemplse table to the
            # instrument table based on exchange and symbol
            sql = """INSERT INTO instruments (ex_symbol, exchange, market, name, symbol)
            SELECT t.ex_symbol, t.exchange, t.market, t.name, t.symbol
            FROM instrumentstemplse t
            WHERE NOT EXISTS 
                (SELECT 1 FROM instruments f
                 WHERE t.ex_symbol = f.ex_symbol)"""
            conn.execute(sql)

            # Delete instruments from table instruments that are no longer present
            # in instrumenttemplse based on symbol name
            sql = """DELETE FROM instruments
                    WHERE NOT EXISTS
                    (SELECT NULL FROM instrumentstemplse
                     Where instruments.ex_symbol = instrumentstemplse.ex_symbol)
                     AND instruments.exchange = 'LSE'"""
            conn.execute(sql)

            # Update exchange, market and name columns in the instruments table with any
            # changes from instrumenttemplse table based on symbol name
            sql = """update instruments 
                    set    (exchange,market, name)=
                    (SELECT exchange, market, name
                    FROM     instrumentstemplse AS instrumentstemp
                    WHERE    instrumentstemp.ex_symbol = instruments.ex_symbol)
                    WHERE NOT EXISTS
                    (SELECT 1 FROM instrumentstemplse
                     Where instruments.exchange = instrumentstemplse.exchange 
                     AND instruments.market = instrumentstemplse.market
                     AND instruments.name = instrumentstemplse.name)
                     AND instruments.exchange = 'LSE'"""

            conn.execute(sql)
            conn.commit()
            conn.close()

        except:
            logger.error('update_lse_instruments_from_temp: Error connecting to Database')
            logger.debug(traceback.format_exc())
    else:
        logger.warning('update_lse_instruments_from_temp: Database file does not exist')


def lse_instruments():

    load_lse_instruments_from_file()
    update_lse_instruments_from_temp()


def load_nasdaq_instruments_from_file():

    nasdaq = ifp.NASDAQFileParser()
    df = nasdaq.get_database_data()

    if os.path.isfile(config.database_path):
        try:
            conn = sqlite3.connect(config.database_path)
            # Create Temporary SQL table to hold up-to-date NASDAQ Instruments
            df.to_sql(name='instrumentstempnasdaq', con=conn, schema=None, if_exists='replace',
                      index=False, index_label=None, chunksize=None, dtype=None, method=None)
            conn.commit()
            conn.close()

        except:
            logger.error('load_nasdaq_instruments_from_file: Error connecting to Database')
            logger.debug(traceback.format_exc())
    else:
        logger.warning('load_nasdaq_instruments_from_file: Database file does not exist')


def update_nasdaq_instruments_from_temp():

    if os.path.isfile(config.database_path):
        try:
            conn = sqlite3.connect(config.database_path)
            # INSERT any new instruments from the instrumenttempnasdaq table to the
            # instrument table based on exchange and symbol
            sql = """INSERT INTO instruments (ex_symbol, exchange, market, name, symbol)
            SELECT t.ex_symbol, t.exchange, t.market, t.name, t.symbol
            FROM instrumentstempnasdaq t
            WHERE NOT EXISTS 
                (SELECT 1 FROM instruments f
                 WHERE t.ex_symbol = f.ex_symbol)"""
            conn.execute(sql)

            # Delete instruments from table instruments that are no longer present
            # in instrumentstempnasdaq based on symbol name
            sql = """DELETE FROM instruments
                    WHERE NOT EXISTS
                    (SELECT NULL FROM instrumentstempnasdaq
                     Where instruments.ex_symbol = instrumentstempnasdaq.ex_symbol)
                     AND instruments.exchange = 'NASDAQ'"""
            conn.execute(sql)

            # Update exchange, market and name columns in the instruments table with any
            # changes from instrumentstempnasdaq table based on symbol name
            sql = """update instruments 
                    set    (exchange,market, name)=
                    (SELECT exchange, market, name
                    FROM     instrumentstempnasdaq AS instrumentstemp
                    WHERE    instrumentstemp.ex_symbol = instruments.ex_symbol)
                    WHERE NOT EXISTS
                    (SELECT 1 FROM instrumentstempnasdaq
                     Where instruments.exchange = instrumentstempnasdaq.exchange 
                     AND instruments.market = instrumentstempnasdaq.market
                     AND instruments.name = instrumentstempnasdaq.name)
                     AND instruments.exchange = 'NASDAQ'"""

            conn.execute(sql)
            conn.commit()
            conn.close()

        except:
            logger.error('update_nasdaq_instruments_from_temp: Error connecting to Database')
            logger.debug(traceback.format_exc())
    else:
        logger.warning('update_nasdaq_instruments_from_temp: Database file does not exist')


def nasdaq_instruments():

    load_nasdaq_instruments_from_file()
    update_nasdaq_instruments_from_temp()


def load_nyse_instruments_from_file():

    nyse = ifp.NYSEFileParser()
    df = nyse.get_database_data()

    if os.path.isfile(config.database_path):
        try:
            conn = sqlite3.connect(config.database_path)
            # Create Temporary SQL table to hold up-to-date NYSE Instruments
            df.to_sql(name='instrumentstempnyse', con=conn, schema=None, if_exists='replace',
                      index=False, index_label=None, chunksize=None, dtype=None, method=None)
            conn.commit()
            conn.close()

        except:
            logger.error('load_nyse_instruments_from_file: Error connecting to Database')
            logger.debug(traceback.format_exc())
    else:
        logger.warning('load_nyse_instruments_from_file: Database file does not exist')


def update_nyse_instruments_from_temp():

    if os.path.isfile(config.database_path):
        try:
            conn = sqlite3.connect(config.database_path)
            # INSERT any new instruments from the instrumenttempnyse table to the
            # instrument table based on exchange and symbol
            sql = """INSERT INTO instruments (ex_symbol, exchange, market, name, symbol)
            SELECT t.ex_symbol, t.exchange, t.market, t.name, t.symbol
            FROM instrumentstempnyse t
            WHERE NOT EXISTS 
                (SELECT 1 FROM instruments f
                 WHERE t.ex_symbol = f.ex_symbol)"""
            conn.execute(sql)

            # Delete instruments from table instruments that are no longer present
            # in instrumentstempnyse based on symbol name
            sql = """DELETE FROM instruments
                    WHERE NOT EXISTS
                    (SELECT NULL FROM instrumentstempnyse
                     Where instruments.ex_symbol = instrumentstempnyse.ex_symbol)
                     AND instruments.exchange = 'NYSE'"""
            conn.execute(sql)

            # Update exchange, market and name columns in the instruments table with any
            # changes from instrumentstempnyse table based on symbol name
            sql = """update instruments 
                    set    (exchange,market, name)=
                    (SELECT exchange, market, name
                    FROM     instrumentstempnyse AS instrumentstemp
                    WHERE    instrumentstemp.ex_symbol = instruments.ex_symbol)
                    WHERE NOT EXISTS
                    (SELECT 1 FROM instrumentstempnyse
                     Where instruments.exchange = instrumentstempnyse.exchange 
                     AND instruments.market = instrumentstempnyse.market
                     AND instruments.name = instrumentstempnyse.name)
                     AND instruments.exchange = 'NYSE'"""

            conn.execute(sql)
            conn.commit()
            conn.close()

        except:
            logger.error('update_nyse_instruments_from_temp: Error connecting to Database')
            logger.debug(traceback.format_exc())
    else:
        logger.warning('update_nyse_instruments_from_temp: Database file does not exist')


def nyse_instruments():

    load_nyse_instruments_from_file()
    update_nyse_instruments_from_temp()


def all_instruments():
    lse_instruments()
    nasdaq_instruments()
    nyse_instruments()


def load_instrument_data(ex_symbol, df):

    if os.path.isfile(config.database_path):
        try:

            df.columns = ['open', 'high', 'low', 'close', 'volume']
            del df['volume']
            df['date'] = df.index
            df['instrument_pk'] = ex_symbol

            conn = sqlite3.connect(config.database_path)
            # Append instrument data from the dataframe to the SQL Table
            df.to_sql(name='instrumentdata', con=conn, schema=None, if_exists='append',
                      index=False, index_label=None, chunksize=None, dtype=None, method=None)
            conn.commit()
            conn.close()

        except:
            logger.error('load_instrument_data: Error connecting to Database or uploading data')
            logger.debug(traceback.format_exc())
    else:
        logger.warning('load_instrument_data: Database file does not exist')


def update_instruments_data_load_date(ex_symbol):

    if os.path.isfile(config.database_path):
        try:

            today = datetime.datetime.today()

            conn = sqlite3.connect(config.database_path)
            # INSERT any new instruments from the instrumenttempnyse table to the
            # instrument table based on exchange and symbol

            sql = "UPDATE instruments SET data_load_date = (?) WHERE ex_symbol = (?)"
            vals = (today, ex_symbol)
            conn.execute(sql, vals)
            conn.commit()
            conn.close()

        except:
            logger.error('load_instrument_data: Error connecting to Database or uploading data')
            logger.debug(traceback.format_exc())
    else:
        logger.warning('load_instrument_data: Database file does not exist')



