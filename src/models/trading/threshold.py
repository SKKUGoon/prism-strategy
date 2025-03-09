import pandas as pd


class Threshold2Sigma:
    def __init__(self, asset_spread: pd.Series, threshold: float):
        self.asset_spread = asset_spread
        self.threshold = threshold

        self.position = 0

    def position_lifecycle(self):
        # If the spread is greater than the threshold enter position 
        # (self.position = 1 or -1, based on asset1's position respectively)
        #   - If the spread is positive, short asset1 and long asset2 (self.position = -1)
        #   - If the spread is negative, long asset1 and short asset2 (self.position = 1)
        # If the spread goes below the 0 line exit position (self.position = 0)
        columns = ["spread", "asset1", "asset2"]
        life = list()
        for s in self.asset_spread:
            if self.position != 0:
                # Check for position exit
                if self.position == 1:
                    # Current position
                    #   - Short asset1 and long asset2
                    if s < 0:
                        # Exit position
                        life.append([s, "buy", "sell"])
                        self.position = 0
                    else:
                        # Hold position
                        life.append([s, None, None])
                else:
                    if s > 0:
                        # Exit position
                        life.append([s, "sell", "buy"])
                        self.position = 0
                    else:
                        # Hold position
                        life.append([s, None, None])
            else:
                if s > self.threshold:
                    self.position = 1
                    life.append([s, "sell", "buy"])
                elif s < -self.threshold:
                    self.position = -1
                    life.append([s, "buy", "sell"])
                else:
                    life.append([s, None, None])

        return pd.DataFrame(life, columns=columns, index=self.asset_spread.index)
    
    