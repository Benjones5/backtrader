import os

import backtrader

from alpha_vantage.timeseries import TimeSeries


class AlphaVantageFeed(TimeSeries):

    def __init__(self):
        super().__init__(
            key=os.environ['ALPHA_VANTAGE_KEY'],
            output_format='pandas')

    def feed(self, symbol, start_date, end_date, **kwargs):
        output_size = kwargs.pop('output_size', 'full')
        data, _ = self.get_daily(symbol, outputsize=output_size)

        df_data = (
           data
           .sort_index()
           .rename(columns={column:column[3:] for column in data.columns})
           .loc[start_date: end_date]
        )

        return backtrader.feeds.PandasData(dataname=df_data)
