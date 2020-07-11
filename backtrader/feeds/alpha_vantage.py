import os

import backtrader

from alpha_vantage.timeseries import TimeSeries


class AlphaVantageFeed(TimeSeries):

    required_columns = ['open', 'high', 'low', 'close', 'volume']

    def __init__(self):
        super().__init__(
            key=os.environ['ALPHA_VANTAGE_KEY'],
            output_format='pandas')

    def get_daily_feed(self, symbol, start_date, end_date, **kwargs):
        output_size = kwargs.pop('output_size', 'full')
        data, _ = self.get_daily(symbol, outputsize=output_size)

        df_data = (
           data
           .sort_index()
           .rename(columns={
                column:column[3:].replace(' ', '_')
                for column in data.columns})
           .loc[start_date: end_date]
           .filter(self.required_columns, axis=1)
        )

        return backtrader.feeds.PandasData(dataname=df_data)

    def get_daily_adjusted_feed(self, symbol, start_date, end_date, **kwargs):
        output_size = kwargs.pop('output_size', 'full')
        data, _ = self.get_daily_adjusted(symbol, outputsize=output_size)


        df_data = (
           data
           .sort_index()
           .rename(columns={
                   column:column[3:].replace(' ', '_')
                   for column in data.columns})
           .loc[start_date: end_date]
           .assign(
                adj_factor=lambda df: df['adjusted_close'] / df['close'],
                open=lambda df: df['open'] * df['adj_factor'],
                high=lambda df: df['high'] * df['adj_factor'],
                low=lambda df: df['low'] * df['adj_factor'],
                close=lambda df: df['close'] * df['adj_factor'])
           .filter(self.required_columns, axis=1)
        )

        return backtrader.feeds.PandasData(dataname=df_data)
