import config, logging, os, traceback, time, datetime
from utils import log, database_builder, database_loader, database_reader, instruments_collector, instruments_data_collector, analysis
import pandas, numpy, math

logger = logging.getLogger(config.logger_name)


def initialisation():

    log.init_logger()

def init_database():
    database_builder.build_database()
    instruments_collector.get_all_instruments()
    database_loader.all_instruments()


# Reads instruments from database
# Collects instrument data from APIs
# Loads instrument data to database
def read_collect_load_instruments_data():

    # Read database
    database = database_reader.ReadDatabase()
    db_df = database.get_unprocessed_instruments_ex_symbol_symbols_exchange()

    # Collect instrument data from API
    # For each row in results from database
    # Get the symbol
    # send the API request to AlphaVantage for results
    av = instruments_data_collector.AlphaVantage()
    i = 0
    av.create_new_timeseries(config.ALPHA_VANTAGE_keys_List[i])
    for index, row in db_df.iterrows():
        ex_symbol = row['ex_symbol']
        exchange = row['exchange']
        symbol = row['symbol']
        time.sleep(15)

        res = av.get_instrument_data(exchange, symbol)

        if res['error']:
            if res['error'] == "Invalid API call":
                pass
            if res['error'] == "API 5/min or 500/day Limit Reached":
                i += 1
                av.create_new_timeseries(config.ALPHA_VANTAGE_keys_List[i])

        else:
            if res['df'] is not None:
                if not res['df'].empty:

                    # Load the instrument data to the database
                    database_loader.load_instrument_data(ex_symbol, res['df'])
                    database_loader.update_instruments_data_load_date(ex_symbol)


def pct_change_pattern(row):
    if numpy.isnan(row['pct_change']):
        return 9
    if row['pct_change'] >= 5:
        return 2
    if row['pct_change'] <= -5:
        return 1
    if row['pct_change'] >-5 and row['pct_change'] < 5:
        return 0


def run_stock_quarter_analysis():

    analyse = analysis.Analyse()
    df_instruments = analyse.get_instruments()

    for index, row in df_instruments.iterrows():
        ex_symbol = row['ex_symbol']
        exchange = row['exchange']
        symbol = row['symbol']

        df = analyse.get_stock(instrument_pk=ex_symbol)

        quarters_end = [
                        {"month":3, "day":31},
                        {"month":6, "day":30},
                        {"month":9, "day":30},
                        {"month":12, "day":31}
                        ]
        df['year'] = df['date'].str.split(' ').str[0].str.split('-').str[0]
        df['month'] = df['date'].str.split(' ').str[0].str.split('-').str[1]
        df['day'] = df['date'].str.split(' ').str[0].str.split('-').str[2]

        arr_years = df.year.unique()
        # print(df.head())
        # print(arr_years)

        df['date'] = pandas.to_datetime(df['date'])
        df = df.set_index('date')

        arr_quarterly_dates_indexes = []
        for year in arr_years:
            for quarter_end in quarters_end:

                arr_quarterly_dates_indexes.append(df.index.get_loc(
                    datetime.datetime(int(year), quarter_end['month'],quarter_end['day']),method='nearest'))

        arr_quarterly_dates_indexes = sorted(list(set(arr_quarterly_dates_indexes)))[1:-1]

        # print(arr_quarterly_dates_indexes)

        df = df.iloc[arr_quarterly_dates_indexes]
        df = df.iloc[::-1]
        df['pct_change'] = df['close'].pct_change()*100

        df['pct_change_pattern'] = df.apply (lambda row: pct_change_pattern(row), axis=1)
        qtr_pattern = df.pct_change_pattern.astype(str).str.cat()
        qtr_pattern_1 = []
        qtr_pattern_2 = []
        qtr_pattern_3 = []
        qtr_pattern_4 = []

        count = 0
        for i in qtr_pattern:
            if count == 0:
                qtr_pattern_1.append(i)
            if count == 1:
                qtr_pattern_2.append(i)
            if count == 2:
                qtr_pattern_3.append(i)
            if count == 3:
                qtr_pattern_4.append(i)
            count += 1
            if count == 4:
                count = 0
        try:
            if qtr_pattern_1[-4] == qtr_pattern_1[-3] == qtr_pattern_1[-2] == qtr_pattern_1[-1]:

                print (ex_symbol, exchange, symbol, qtr_pattern_1[-4:])
                print (df)
                break
        except:
            pass



        # print(qtr_pattern_2)
        # print(qtr_pattern_3)
        # print(qtr_pattern_4)
        # print(qtr_pattern.index('11'))



