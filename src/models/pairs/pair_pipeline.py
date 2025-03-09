import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint

import itertools
from src.util.extra import timeit


class PairTrading:
    def __init__(self):
        self.start_time_loc = 0
        self.end_time_loc = 0

    def set_interval(self, start_time_loc: int, interval: int):
        self.start_time_loc = start_time_loc
        self.end_time_loc = start_time_loc + interval

    @staticmethod
    def spread(asset1: pd.Series, asset2: pd.Series):
        X = sm.add_constant(asset2)
        model = sm.OLS(asset1, X).fit()
        beta0, beta1 = model.params
        spread = asset1 - (beta0 + beta1 * asset2)

        return spread

    @staticmethod
    def hurst_exponent(time_series: pd.Series, min_lag: int = 2, max_lag: int = 60):
        """
        Lags - Represent different time intervals. 
            For each lag, the function calculates the STDEV of the difference between the time series and the shifted version.
            List Tau contains quantified measure of how spread out the values of the particular lag
        Log-Log Relationship. 
            - Variability scales with the lag according to a power law. 
            Taking logarithm on both sides to geta linear relationship.
        H - Hurst Exponent. Timeseries's behavior. 
            - H < 0.5 - Mean Reversion. STD increase slowly than the square root of the lag. Series is do not persist strongly. Corrected over time.
            - H > 0.5 - Persistent (Trending)
            - H = 0.5 - Random Walk. STD grows proportationally to the square root of the lag. (Random Walk, variance grows linearly with time)
        """
        lags = range(min_lag, max_lag)
        # Calculate the standard deviation of the difference for each lag
        tau = [np.std(np.subtract(time_series.iloc[lag:], time_series.iloc[:-lag])) for lag in lags]
        tau = np.array(tau)
        
        # Replace zero values with a small number to avoid log(0)
        tau[tau == 0] = 1e-8

        # Fit a line to the log-log plot of lags vs. tau
        poly_coeffs = np.polyfit(np.log(list(lags)), np.log(tau), 1)
        H = poly_coeffs[0]
        return H

    @timeit
    def pipeline(self, asset_prices: pd.DataFrame):
        pairs = set()

        # Get all possible pairs of assets
        assets = asset_prices.columns
        candidate = list(itertools.combinations(assets, 2))

        # Slice the asset_prices up
        price_path = asset_prices.iloc[self.start_time_loc : self.end_time_loc]
        for a1, a2 in candidate:
            a1_price_path = price_path[a1]
            a2_price_path = price_path[a2]

            if len(a1_price_path.dropna()) != len(a2_price_path.dropna()):
                continue

            # Calculate Spread
            spread = self.spread(a1_price_path, a2_price_path)

            # Calculate cointegration
            _score, pvalue, _ = coint(a1_price_path, a2_price_path)

            # Calculate Hurst Exponent
            hurst = self.hurst_exponent(spread, 2, 60)

            condition1 = pvalue < 0.05  # Series are cointegrated
            condition2 = hurst < 0.5  # Series is mean reverting
            if condition1 and condition2:
                print(f"{a1} - {a2} have made the cut on {self.start_time_loc} ~ {self.end_time_loc}")
                pairs.add((a1, a2))
            
        return pairs