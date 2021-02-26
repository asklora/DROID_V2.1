import sys
import pandas as pd
import datetime as dt
from general.sql_query import read_query
from general.table_name import get_bot_uno_backtest_table_name, get_bot_ucdc_backtest_table_name

def get_pnl_data(table_name):
    query = f"select spot_date, ticker, pnl, spot_price, option_type from {table_name} "
    data = read_query(query, table=table_name, cpu_counts=True)
    if len(data) == 0:
        print(f"We don't have executive data in {data}.")
        sys.exit()
    return data

uno_df = get_pnl_data(get_bot_uno_backtest_table_name())
uno_mod_df = get_pnl_data(get_bot_uno_backtest_table_name() + '_mod')
ucdc_df = get_pnl_data(get_bot_ucdc_backtest_table_name())
ucdc_mod_df = get_pnl_data(get_bot_ucdc_backtest_table_name() + '_mod')

uno_df_itm = uno_df[uno_df.option_type == 'ITM']
uno_df_otm = uno_df[uno_df.option_type == 'OTM']

ucdc_df_atmd5 = ucdc_mod_df[ucdc_mod_df.option_type == 'ATMd5']
ucdc_df_atmv10 = ucdc_mod_df[ucdc_mod_df.option_type == 'ATMv10']

uno_df_itmv10 = uno_mod_df[uno_mod_df.option_type == 'ITMv10']
uno_df_otmv10 = uno_mod_df[uno_mod_df.option_type == 'OTMv10']
uno_df_itmd5 = uno_mod_df[uno_mod_df.option_type == 'ITMd5']
uno_df_otmd5 = uno_mod_df[uno_mod_df.option_type == 'OTMd5']

uno_df_itm.rename(columns={'pnl': 'pnl_uno_itm'}, inplace=True)
uno_df_otm.rename(columns={'pnl': 'pnl_uno_otm'}, inplace=True)
ucdc_df.rename(columns={'pnl': 'pnl_ucdc'}, inplace=True)
ucdc_df_atmd5.rename(columns={'pnl': 'pnl_ucdc_atmd5'}, inplace=True)
ucdc_df_atmv10.rename(columns={'pnl': 'pnl_ucdc_atmv10'}, inplace=True)
uno_df_itmv10.rename(columns={'pnl': 'pnl_uno_itmv10'}, inplace=True)
uno_df_otmv10.rename(columns={'pnl': 'pnl_uno_otmv10'}, inplace=True)
uno_df_itmd5.rename(columns={'pnl': 'pnl_uno_itmd5'}, inplace=True)
uno_df_otmd5.rename(columns={'pnl': 'pnl_uno_otmd5'}, inplace=True)

uno_df_itm.drop_duplicates(subset=['ticker', 'spot_date'], keep="last", inplace=True)
uno_df_otm.drop_duplicates(subset=['ticker', 'spot_date'], keep="last", inplace=True)
ucdc_df.drop_duplicates(subset=['ticker', 'spot_date'], keep="last", inplace=True)
ucdc_df_atmd5.drop_duplicates(subset=['ticker', 'spot_date'], keep="last", inplace=True)
ucdc_df_atmv10.drop_duplicates(subset=['ticker', 'spot_date'], keep="last", inplace=True)
uno_df_itmv10.drop_duplicates(subset=['ticker', 'spot_date'], keep="last", inplace=True)
uno_df_otmv10.drop_duplicates(subset=['ticker', 'spot_date'], keep="last", inplace=True)
uno_df_itmd5.drop_duplicates(subset=['ticker', 'spot_date'], keep="last", inplace=True)
uno_df_otmd5.drop_duplicates(subset=['ticker', 'spot_date'], keep="last", inplace=True)

final_df = uno_df_itm.merge(uno_df_otm, on=['ticker', 'spot_date'], how='inner')
final_df = final_df.merge(ucdc_df, on=['ticker', 'spot_date'], how='inner')
final_df = final_df.merge(ucdc_df_atmd5, on=['ticker', 'spot_date'], how='inner')
final_df = final_df.merge(ucdc_df_atmv10, on=['ticker', 'spot_date'], how='inner')
final_df = final_df.merge(uno_df_itmv10, on=['ticker', 'spot_date'], how='inner')
final_df = final_df.merge(uno_df_otmv10, on=['ticker', 'spot_date'], how='inner')
final_df = final_df.merge(uno_df_itmd5, on=['ticker', 'spot_date'], how='inner')
final_df = final_df.merge(uno_df_otmd5, on=['ticker', 'spot_date'], how='inner')

final_df = final_df.dropna()

final_df.pnl_uno_itm = final_df.pnl_uno_itm / final_df.spot_price
final_df.pnl_uno_otm = final_df.pnl_uno_otm / final_df.spot_price
final_df.pnl_ucdc = final_df.pnl_ucdc / final_df.spot_price
final_df.pnl_ucdc_atmd5 = final_df.pnl_ucdc_atmd5 / final_df.spot_price
final_df.pnl_ucdc_atmv10 = final_df.pnl_ucdc_atmv10 / final_df.spot_price
final_df.pnl_uno_itmv10 = final_df.pnl_uno_itmv10 / final_df.spot_price
final_df.pnl_uno_otmv10 = final_df.pnl_uno_otmv10 / final_df.spot_price
final_df.pnl_uno_itmd5 = final_df.pnl_uno_itmd5 / final_df.spot_price
final_df.pnl_uno_otmd5 = final_df.pnl_uno_otmd5 / final_df.spot_price

final_stats = pd.DataFrame(index=range(4))
final_stats.loc[0, 'Year'] = 'All'
final_stats.loc[1, 'Year'] = '2018'
final_stats.loc[2, 'Year'] = '2019'
final_stats.loc[3, 'Year'] = '2020'

cols = ['pnl_uno_itm', 'pnl_uno_otm', 'pnl_ucdc', 'pnl_ucdc_atmd5', 'pnl_ucdc_atmv10',
        'pnl_uno_itmv10', 'pnl_uno_otmv10', 'pnl_uno_itmd5', 'pnl_uno_otmd5']

uno_df_itm.rename(columns={'pnl': 'pnl_uno_itm'}, inplace=True)
uno_df_otm.rename(columns={'pnl': 'pnl_uno_otm'}, inplace=True)
ucdc_df.rename(columns={'pnl': 'pnl_ucdc'}, inplace=True)
ucdc_df_atmd5.rename(columns={'pnl': 'pnl_ucdc_atmd5'}, inplace=True)
ucdc_df_atmv10.rename(columns={'pnl': 'pnl_ucdc_atmv10'}, inplace=True)
uno_df_itmv10.rename(columns={'pnl': 'pnl_uno_itmv10'}, inplace=True)
uno_df_otmv10.rename(columns={'pnl': 'pnl_uno_otmv10'}, inplace=True)
uno_df_itmd5.rename(columns={'pnl': 'pnl_uno_itmd5'}, inplace=True)
uno_df_otmd5.rename(columns={'pnl': 'pnl_uno_otmd5'}, inplace=True)

for col in cols:
    final_stats.loc[0, col + '_mean'] = final_df[col].mean()
    final_stats.loc[0, col + '_std'] = final_df[col].std()
    final_stats.loc[1, col + '_mean'] = final_df.loc[(final_df.spot_date >= dt.date(2018, 1, 1)) &
                                                     (final_df.spot_date < dt.date(2019, 1, 1)), col].mean()
    final_stats.loc[1, col + '_std'] = final_df.loc[(final_df.spot_date >= dt.date(2018, 1, 1)) &
                                                     (final_df.spot_date < dt.date(2019, 1, 1)), col].std()
    final_stats.loc[2, col + '_mean'] = final_df.loc[(final_df.spot_date >= dt.date(2019, 1, 1)) &
                                                     (final_df.spot_date < dt.date(2020, 1, 1)), col].mean()
    final_stats.loc[2, col + '_std'] = final_df.loc[(final_df.spot_date >= dt.date(2019, 1, 1)) &
                                                     (final_df.spot_date < dt.date(2020, 1, 1)), col].std()
    final_stats.loc[3, col + '_mean'] = final_df.loc[(final_df.spot_date >= dt.date(2020, 1, 1)), col].mean()
    final_stats.loc[3, col + '_std'] = final_df.loc[(final_df.spot_date >= dt.date(2020, 1, 1)), col].std()

final_stats.to_csv('final_stats.csv')