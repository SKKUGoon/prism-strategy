from typing import Literal, Tuple


# class Asset:
#     def __init__(self, name: str, price: float, quantity: float, side: Literal["buy", "sell"]):
#         if side == "sell":
#             quantity = -1 * quantity

#         self.name = name
#         self.price = price
#         self.quantity = quantity
#         self.side = side

#     def __repr__(self):
#         return f"{self.name} ({self.side}): {self.price} | {self.quantity} (Total: {round(self.price * self.quantity, 3)})"

#     def liquidate(self, price, quantity_ratio: float = 1.0):
#         """Opposite side order"""
#         liquidated_profit = (price - self.price) * (self.quantity * quantity_ratio)

#         # Update quantity
#         self.quantity = self.quantity * (1 - quantity_ratio) if 1 - quantity_ratio > 0 else 0

#         # Return
#         return liquidated_profit


class AccountAsset:
    def __init__(self, 
                 name: str, 
                 acquired_price: float, 
                 quantity: float, 
                 side: Literal["buy", "sell"], 
                 fee: float = 0.0, 
                 slippage: float = 0.0):
        
        self.name = name
        self.book_price = acquired_price
        self.quantity = quantity
        self.side = side

        self.buy_sell_normalizer = 2 * (self.side == "buy") - 1

        # Constants
        self.fee = fee
        self.slippage = slippage

        # Accumulated losses
        self.accumulated_fee = self.book_price * self.quantity * self.fee
        self.accumulated_slippage = self.book_price * self.quantity * self.slippage

    def acquire(self,
                price,
                quantity: float,
                fee: float = 0.0,
                slippage: float = 0.0) -> None:
        self.book_price = ((self.book_price * self.quantity) + (price * quantity)) / (self.quantity + quantity)  # Use weighted average
        self.quantity += quantity

        self.accumulated_fee += price * quantity * fee
        self.accumulated_slippage += price * quantity * slippage

    def liquidate(self, 
                  exit_price, 
                  quantity_ratio: float = 1.0, 
                  fee: float = 0.0, 
                  slippage: float = 0.0) -> Tuple[float, float]:
        """Opposite side order"""
        # Profit calculation
        original_value = self.book_price * self.quantity * quantity_ratio
        liquidated_value = exit_price * (self.quantity * quantity_ratio)
        profit = (liquidated_value - original_value) * self.buy_sell_normalizer
        
        # Update accumulated fees and slippage
        self.accumulated_fee += liquidated_value * fee
        self.accumulated_slippage += liquidated_value * slippage
        total_fee = self.accumulated_fee + self.accumulated_slippage

        # Return
        self.quantity = self.quantity * (1 - quantity_ratio) if 1 - quantity_ratio > 0 else 0
        
        return original_value + profit - total_fee, profit - total_fee        


if __name__ == "__main__":
    # Test Asset
    # a1_path = [10, 13]
    # a2_path = [11, 13]

    # a1 = Asset("a1", a1_path[0], 10, "buy")
    # a2 = Asset("a2", a2_path[0], 10, "sell")

    # a1_profit = a1.liquidate(a1_path[1], 1)
    # a2_profit = a2.liquidate(a2_path[1], 1)

    # long_short_profit = a1_profit + a2_profit
    # long_short_input = a1_path[0] * 10 + a2_path[0] * 10
    # long_short_return = long_short_profit / long_short_input


    # Test AccountAsset
    a1_path = [10, 13, 15]
    a2_path = [11, 13, 15]

    a1 = AccountAsset("a1", a1_path[0], 10, "buy", 0.0004)
    a2 = AccountAsset("a2", a2_path[0], 10, "sell", 0.0004)

    a1.acquire(a1_path[1], 2, 0.0004)
    a2.acquire(a2_path[1], 2, 0.0004)


    a1.liquidate(a1_path[2], 1, 0.0004)
    a2.liquidate(a2_path[2], 1, 0.0004)
    