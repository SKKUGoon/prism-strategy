import pandas as pd

from typing import TypedDict, List
from collections import deque


class OrderbookImbalanceBar(TypedDict, total=False):
    id: int = 0
    start_time: pd.Timestamp = pd.Timestamp.min
    end_time: pd.Timestamp = pd.Timestamp.min
    
    # Price
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    vwap: float = 0.0
    
    # Volume
    row_count: int = 0
    
    # Imbalance
    imbalance_path: List[float] = []
    cumulative_imbalance: float = 0.0
    threshold: float = 0.0


def orderbook_imbalance_information_bar(df: pd.DataFrame,
                                        initial_collection: int,
                                        b_t_ewma: float,
                                        tsize_t_ewma: float,
                                        historical_threshold_limit: float = 0.2,
                                        historical_threshold_collection: int = 100) -> pd.DataFrame:
    bars = []
    historical_threshold = deque(maxlen=historical_threshold_collection)
    current_bar = OrderbookImbalanceBar()

    # Ensure the data is sorted by datetime index
    df = df.sort_index()

    # Initialize the bar with the first few data (initial collection)
    genesis_start = df.index.min()
    genesis_end = genesis_start + pd.Timedelta(seconds=initial_collection)
    genesis_data = df.loc[genesis_start:genesis_end]
    
    # Calculate the initial threshold using the first few data
    genesis_imbalance_path_time  = (genesis_data['non_spoofed_best_ask_volume'] - genesis_data['non_spoofed_best_bid_volume']).index.tolist()
    genesis_imbalance_path = (genesis_data['non_spoofed_best_ask_volume'] - genesis_data['non_spoofed_best_bid_volume']).cumsum().tolist()
    genesis_imbalance = (genesis_data['non_spoofed_best_ask_volume'] - genesis_data['non_spoofed_best_bid_volume']).sum()
    genesis_tick_count = genesis_data.shape[0]
    b_t = genesis_imbalance / genesis_tick_count
    tsize_t = genesis_tick_count
    threshold_t = abs(b_t * tsize_t)
    historical_threshold.append(threshold_t)
    
    # Initialize the bar with the first data
    current_bar['id'] = 0
    current_bar['start_time'] = genesis_start
    current_bar['end_time'] = genesis_end
    current_bar['open'] = genesis_data['price'].iloc[0]
    current_bar['high'] = genesis_data['price'].max()
    current_bar['low'] = genesis_data['price'].min()
    current_bar['close'] = genesis_data['price'].iloc[-1]
    current_bar['row_count'] = genesis_tick_count
    current_bar['imbalance_path'] = [genesis_imbalance_path_time, genesis_imbalance_path]
    current_bar['cumulative_imbalance'] = genesis_imbalance
    current_bar['threshold'] = None  # First bar has no threshold
    bars.append(current_bar)

    # Reset the current bar
    current_bar = OrderbookImbalanceBar()
    current_bar['threshold'] = threshold_t
    imbalance_path_time, imbalance_path = [], []
    bar_id = 1
    
    previous_price, previous_bid_vol, previous_ask_vol = (
        genesis_data['price'].iloc[-1], 
        genesis_data['non_spoofed_best_bid_volume'].iloc[-1], 
        genesis_data['non_spoofed_best_ask_volume'].iloc[-1],
    )

    for idx, row in df.iterrows():
        if idx < genesis_end:
            continue  # Skip the genesis data

        price = row['price']
        bid_vol = row['non_spoofed_best_bid_volume']
        ask_vol = row['non_spoofed_best_ask_volume']
        imbalance = bid_vol - ask_vol

        if (price == previous_price) and (bid_vol == previous_bid_vol) and (ask_vol == previous_ask_vol):
            continue  # No changes in price, bid volume or ask volume. No information given.

        # Build the curreent bar
        if current_bar.get('start_time', pd.Timestamp.min) == pd.Timestamp.min:
            current_bar['start_time'] = idx
            current_bar['open'] = price
            current_bar['high'] = price
            current_bar['low'] = price
        
        current_bar['end_time'] = idx
        current_bar['high'] = current_bar['high'] if current_bar['high'] > price else price
        current_bar['low'] = current_bar['low'] if current_bar['low'] < price else price
        current_bar['close'] = price
        current_bar['row_count'] = current_bar.get('row_count', 0) + 1
        current_bar['cumulative_imbalance'] = current_bar.get('cumulative_imbalance', 0.0) + imbalance
        
        imbalance_path.append(imbalance)
        imbalance_path_time.append(idx)

        if abs(current_bar.get('cumulative_imbalance', 0.0)) > threshold_t:
            current_bar['id'] = bar_id
            current_bar['imbalance_path'] = [
                imbalance_path_time,
                pd.Series(imbalance_path).cumsum().tolist()
            ]

            bars.append(current_bar)
            print("new bar", bar_id)

            # Calculate the next threshold
            b_t = (current_bar['cumulative_imbalance'] / current_bar['row_count']) * b_t_ewma + (1 - b_t_ewma) * b_t
            tsize_t = current_bar['row_count'] * tsize_t_ewma + (1 - tsize_t_ewma) * tsize_t
            threshold_t = abs(b_t * tsize_t)
            
            # Capping the threshold based on the historical threshold limit
            threshold_upper_limit = max(historical_threshold) * (1 + historical_threshold_limit)
            threshold_lower_limit = min(historical_threshold) * (1 - historical_threshold_limit)
            threshold_t = min(max(threshold_t, threshold_lower_limit), threshold_upper_limit)
            historical_threshold.append(threshold_t)

            # Reset the bar parameter
            current_bar = OrderbookImbalanceBar()
            current_bar['threshold'] = threshold_t
            bar_id += 1
            imbalance_path_time, imbalance_path = [], []

    if bars[-1]['id'] != bar_id:
        # Append the last bar - in progress
        print("appending the last bar - in progress", bar_id)
        current_bar['id'] = bar_id
        current_bar['imbalance_path'] = pd.Series(imbalance_path).cumsum().tolist()
        bars.append(current_bar)

    return pd.DataFrame(bars)