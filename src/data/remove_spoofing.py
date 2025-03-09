import pandas as pd
from typing import Literal

def detect_spoofing(price_series: pd.Series, volume_series: pd.Series, slack: int =1):
    """
    Detect potential spoofing patterns in volume changes
    
    Args:
        volume_series: pandas Series with volume data
        slack: time window in seconds to look for matching opposite changes
        
    Returns:
        set: indices of detected spoofing instances
    """
    diff = volume_series.diff()
    
    diff_df = pd.DataFrame({"price": price_series, "diff": diff})
    diff_df = diff_df[diff_df['diff'] != 0].dropna()

    spoofing_log = set()

    for idx, row in diff_df.iterrows():
        end_idx = idx + pd.Timedelta(seconds=slack)
        target_value = row['diff']
        target_price = row['price']

        potential_matches = diff_df[
            (diff_df.index > idx) & 
            (diff_df.index <= end_idx) &
            (diff_df['price'] == target_price)
        ]

        matches = potential_matches['diff'] == -1 * target_value
        matches = potential_matches[matches]
        
        if len(matches) > 0:
            spoofing_log.add(idx)  # Add the beginning index of the spoofing instance
            for match_idx in matches.index.tolist():
                spoofing_log.add(match_idx)

    spoofing = pd.DataFrame(index=list(spoofing_log))
    spoofing["spoofed"] = [True] * len(spoofing)
    spoofing.sort_index(inplace=True)
    spoofing.index.name = "timestamp"

    return spoofing