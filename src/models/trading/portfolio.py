from typing import Literal, Dict, List
# from src.models.trading.asset import Asset
from src.models.trading.asset import AccountAsset
import uuid

# DEPRECATED: This class is deprecated and will be removed in a future version.
# Use AccountPortfolio instead.
# class Portfolio:
#     def __init__(self):
#         self.portfolio: Dict[str, Asset] = dict()

#     def enter_position(self, asset: str, price: float, quantity: float, side: Literal["buy", "sell"]):
#         self.portfolio[asset] = Asset(asset, price, quantity, side)

#     def exit_position(self, asset: str, exit_price: float, quantity_ratio: float):
#         if self.portfolio.get(asset, None) is None:
#             return 0
        
#         profit = self.portfolio[asset].liquidate(exit_price, quantity_ratio)
        
#         if self.portfolio[asset].quantity == 0:
#             # Remove asset from portfolio
#             del self.portfolio[asset]
#         return profit
    
#     def pair_enter(self,
#                    long_asset: str,
#                    long_price: float,
#                    long_quantity: float,
#                    short_asset: str,
#                    short_price: float,
#                    short_quantity: float):
        
#         self.enter_position(long_asset, long_price, long_quantity, "buy")
#         self.enter_position(short_asset, short_price, short_quantity, "sell")

#     def pair_exit(self,
#                   long_asset: str,
#                   long_exit_price: float,
#                   long_quantity_ratio: float,
#                   short_asset: str,
#                   short_exit_price: float,
#                   short_quantity_ratio: float):
        
#         long_profit = self.exit_position(long_asset, long_exit_price, long_quantity_ratio)
#         short_profit = self.exit_position(short_asset, short_exit_price, short_quantity_ratio)

#         return long_profit + short_profit
    
    
class Order:
    def __init__(self, asset: str, price: float, quantity_ratio: float, side: Literal["buy", "sell"] | None = None):
        self.asset = asset
        self.price = price
        self.quantity_ratio = quantity_ratio
        self.side = side


class AccountPortfolio:
    def __init__(self, initial_cash: float = 1000.0):
        self.assets: Dict[str, Dict[str, AccountAsset]] = dict()  # {trade_id: {asset: AccountAsset}}
        self.cash = initial_cash

    def __repr__(self):
        a = list()
        for trade_id, assets in self.assets.items():
            a.append(f"Trade {trade_id}")
            for asset, asset_info in assets.items():
                a.append(f" - {asset}: {abs(asset_info.book_price * asset_info.quantity)}")

        astr = "\n".join(a)

        repr = f"""
Account Portfolio

Trades: {len(self.assets)}
Cash: {self.cash}

{astr}
"""
        return repr

    def enter_position(self, 
                       id: str,
                       order: Order, 
                       fee: float = 0.0, 
                       slippage: float = 0.0):
        if self.cash < 10:
            print("Order failed: Not enough cash")
            return False

        # Calculate quantity
        budget = max(self.cash * order.quantity_ratio, 10)  # At least 10 quoting assets (USDT)
        quantity = budget / order.price

        if self.assets.get(id, None) is None:
            # Create new trade_id
            new_asset = AccountAsset(order.asset, order.price, quantity, order.side, fee, slippage)
            self.assets[id] = {order.asset: new_asset}
        else:
            if self.assets[id].get(order.asset, None) is None:
                # Add new asset to existing trade_id
                new_asset = AccountAsset(order.asset, order.price, quantity, order.side, fee, slippage)
                self.assets[id][order.asset] = new_asset
            else:
                # Add quantity to existing asset
                self.assets[id][order.asset].acquire(order.price, quantity, fee, slippage)
        
        # Update cash
        self.cash -= budget
        
        return True

    def exit_position(self,
                      id: str,
                      exit_order: Order,
                      fee: float = 0.0, 
                      slippage: float = 0.0):
        if self.assets.get(id, None) is None:
            # No trade_id in portfolio
            print("Exit failed: No trade_id in portfolio")
            return False
        
        if self.assets[id].get(exit_order.asset, None) is None:
            # No asset in portfolio
            print("Exit failed: No asset in portfolio")
            return False
        revenue_with_fee, _profit_with_fee = self.assets[id][exit_order.asset].liquidate(exit_order.price, exit_order.quantity_ratio, fee, slippage)
        # print("add revenue", revenue_with_fee, "profit", _profit_with_fee)
        self.cash += revenue_with_fee

        # Remove asset from portfolio
        if self.assets[id][exit_order.asset].quantity == 0:
            del self.assets[id][exit_order.asset]

        # Remove trade_id from portfolio
        if len(self.assets[id]) == 0:
            del self.assets[id]

        return True

    def pair_enter(self, order1: Order, order2: Order, fee: float = 0.0, slippage: float = 0.0) -> str:  # Retu
        key = uuid.uuid4()

        # Enter position
        success1 = self.enter_position(str(key), order1, fee, slippage)
        success2 = self.enter_position(str(key), order2, fee, slippage)

        if not success1 and not success2:
            print("Failed to enter pair position")
            return None

        # Return pair key
        return str(key)

    def pair_exit(self, id: str, long_order: Order, short_order: Order, fee: float = 0.0, slippage: float = 0.0):
        success1 = self.exit_position(id, long_order, fee, slippage)
        success2 = self.exit_position(id, short_order, fee, slippage)

        if not success1 and not success2:
            print("Failed to exit pair position")
            return None

        return True
    
    def force_exit_by_name(self, a1: str, price: float, fee: float = 0.0, slippage: float = 0.0):
        for trade_id, trades in self.assets.items():
            if trades.get(a1, None):
                # Liquidate and add revenue
                revenue_with_fee, _profit_with_fee = trades.get(a1).liquidate(price, 1, fee, slippage)
                self.cash += revenue_with_fee

                print(f"FORCE LIQUIDATION {trade_id}. {a1}. for {round(price, 4)}")

    def force_remove_by_id(self, id: str):
        del self.assets[id]
            
        
# if __name__ == "__main__":
#     p = Portfolio()
#     p.pair_enter("a1", 10, 10, "a2", 10, 10)
#     p.pair_exit("a1", 11, 1, "a2", 9, 1)  # Remember that it uses ratio
