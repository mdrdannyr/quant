import os

# project settings
ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) # This is your Project Root

# logger settings
logger_name = "quant"
log_file_path = os.path.join(ROOT_DIR, 'logs/quant.log')

# database settings
database_path = os.path.join(ROOT_DIR, 'database/quant.db')

# London Stock Exchange properties
lse_url = 'https://www.londonstockexchange.com/statistics/companies-and-issuers/instruments-defined-by-mifir-identifiers-list-on-lse.xlsx'
lse_instruments_filename = os.path.join(ROOT_DIR, 'raw-data/lse_instruments.xlsx')
lse_instruments_main_sheetname = '1.0 All Equity'

# NASDAQ properties
nasdaq_url = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
nasdaq_instruments_filename = os.path.join(ROOT_DIR, 'raw-data/nasdaq_instruments.txt')

# NYSE properties
nyse_url = 'http://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt'
nyse_instruments_filename = os.path.join(ROOT_DIR, 'raw-data/nyse_instruments.txt')

# ALPHA VANTAGE
ALPHA_VANTAGE_key = 'ADD_KEY'
ALPHA_VANTAGE_keys_List = ['ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY',
                           'ADD_KEY']
ALPHA_USAGE_MINUTE_LIMIT = 5
ALPHA_USAGE_DAILY_LIMIT = 500

