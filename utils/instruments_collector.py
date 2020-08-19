import config, logging, os, traceback
import requests

logger = logging.getLogger(config.logger_name)


def get_lse_instruments():

    if not os.path.isfile(config.lse_instruments_filename):
        try:
            url = config.lse_url
            r = requests.get(url)
            open(config.lse_instruments_filename, 'wb').write(r.content)
        except:
            logger.error('get_lse_instruments: Error downloading lse instruments file')
            logger.debug(traceback.format_exc())
    else:
        logger.warning('get_lse_instruments: lse instruments file already exists')


def get_nasdaq_instruments():

    if not os.path.isfile(config.nasdaq_instruments_filename):
        try:
            url = config.nasdaq_url
            r = requests.get(url)
            open(config.nasdaq_instruments_filename, 'wb').write(r.content)
        except:
            logger.error('get_nasdaq_instruments: Error downloading nasdaq instruments file')
            logger.debug(traceback.format_exc())
    else:
        logger.warning('get_nasdaq_instruments: nasdaq instruments file already exists')


def get_nyse_instruments():

    if not os.path.isfile(config.nyse_instruments_filename):
        try:
            url = config.nyse_url
            r = requests.get(url)
            open(config.nyse_instruments_filename, 'wb').write(r.content)
        except:
            logger.error('get_nyse_instruments: Error downloading nyse instruments file')
            logger.debug(traceback.format_exc())
    else:
        logger.warning('get_nyse_instruments: nyse instruments file already exists')


def get_all_instruments():

    get_lse_instruments()
    get_nasdaq_instruments()
    get_nyse_instruments()
