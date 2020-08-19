import config, logging, os, traceback
import sqlite3

logger = logging.getLogger(config.logger_name)

##################################################################################################
# Raw Column Headers
# LSE       TIDM	Issuer Name	Instrument Name	ISIN	MiFIR Identifier Code	MiFIR Identifier Description	ICB Industry	ICB Super-Sector Name	Start date	Country of Incorporation	Trading Currency	LSE Market	FCA Listing Category	Market Segment Code	Market Sector Code
# NASDAQ    Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
# NYSE ACT  Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol
##################################################################################################

#################################################
##                                tbl.instruments
#################################################
#########PK id |symbol| name            |  exchange |  market
#LSE           | TIDM | Issuer Name     | 'LSE'     |LSE Market
#NASDAQ        |Symbol| Security Name   |'NASDAQ'   |Market Category
#NYSE          |Symbol| Security Name   |'NYSE'     |Market Category
#################################################



#################################################################################
##                                                            tbl.instrumentsdata
#################################################################################
#PK id | FK instrument id (tbl.instruments.id) | Date | high | low | open | close
#################################################################################

def build_database_file():

    if not os.path.isfile(config.database_path):
        try:
            conn = sqlite3.connect(config.database_path)
            conn.commit()
            conn.close()
        except:
            logger.error('build_database: Error connecting to Database')
            logger.debug(traceback.format_exc())

    else:
        logger.warning('build_database: Database file already exists')


def build_table_instruments():
    # Build tbl.instruments
    if os.path.isfile(config.database_path):
        try:
            conn = sqlite3.connect(config.database_path)
            c = conn.cursor()

            # Check if table exists
            c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='instruments' ''')

            # if the count is 1, then table exists
            if c.fetchone()[0] == 0:
                try:
                    c.execute(
                        ''' CREATE TABLE instruments
                        ([ex_symbol] text PRIMARY KEY NOT NULL UNIQUE,
                        [symbol] text NOT NULL,
                        [exchange] text NOT NULL, 
                        [market] text NOT NULL, 
                        [name] text NOT NULL,
                        [data_load_date] date)'''
                            )
                except:
                    logger.error('build_table_instruments: Error creating table instrument')
                    logger.debug(traceback.format_exc())

            else:
                logger.warning('build_table_instruments: table instruments exists')

        except:
            logger.error('build_table_instruments: Error connecting to Database')
            logger.debug(traceback.format_exc())
        else:
            conn.commit()
            conn.close()
    else:
        logger.error('build_table_instruments: Database file does not exist')
        logger.debug(traceback.format_exc())


def build_table_instruments_data():
    # Build tbl.instruments_name
    if os.path.isfile(config.database_path):
        try:
            conn = sqlite3.connect(config.database_path)
            c = conn.cursor()

            # Check if table exists
            c.execute(" SELECT count(name) FROM sqlite_master WHERE type='table' AND name='instrumentdata'")

            # if the count is 1, then table exists
            if c.fetchone()[0] == 0:
                try:
                    c.execute(" CREATE TABLE instrumentdata "
                              "([id] INTEGER PRIMARY KEY NOT NULL UNIQUE, "
                              "[instrument_pk] text NOT NULL, [date] date NOT NULL, "
                              "[high] real, [low] real, [open] real, [close] real, "
                              "CONSTRAINT fk_instruments "
                              "FOREIGN KEY (instrument_pk)"
                              "REFERENCES instruments (ex_symbol) "
                              "ON DELETE CASCADE)"
                              )
                except:
                    logger.error('build_table_instruments_name: Error creating table instrument')
                    logger.debug(traceback.format_exc())
            else:
                logger.warning('build_table_instruments_name: table instruments exists')
        except:
            logger.error('build_table_instruments_name: Error connecting to Database')
            logger.debug(traceback.format_exc())
        else:
            conn.commit()
            conn.close()
    else:
        logger.error('build_table_instruments_name: Database file does not exist')
        logger.debug(traceback.format_exc())


def build_database():

    build_database_file()
    build_table_instruments()
    build_table_instruments_data()