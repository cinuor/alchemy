# import json
# import os
import decimal
import time

# import csv
from typing import Dict, List
from binance.client import Client
from binance import ThreadedWebsocketManager
from binance.enums import HistoricalKlinesType
from utils.ts import timestamp

INTERVAL = {
    "1m": Client.KLINE_INTERVAL_1MINUTE,
    "3m": Client.KLINE_INTERVAL_3MINUTE,
    "5m": Client.KLINE_INTERVAL_5MINUTE,
    "15m": Client.KLINE_INTERVAL_15MINUTE,
    "30m": Client.KLINE_INTERVAL_30MINUTE,
    "1h": Client.KLINE_INTERVAL_1HOUR,
    "2h": Client.KLINE_INTERVAL_2HOUR,
    "4h": Client.KLINE_INTERVAL_4HOUR,
    "6h": Client.KLINE_INTERVAL_6HOUR,
    "8h": Client.KLINE_INTERVAL_8HOUR,
    "12h": Client.KLINE_INTERVAL_12HOUR,
    "1d": Client.KLINE_INTERVAL_1DAY,
    "3d": Client.KLINE_INTERVAL_3DAY,
    "1w": Client.KLINE_INTERVAL_1DAY,
    "1M": Client.KLINE_INTERVAL_1MONTH,
}


def _f(*args, **kwargs):
    pass


CallbackType = type(_f)


class BinanceClient:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        fee: str = "0.001",
    ):
        self.client = Client(api_key=api_key, api_secret=api_secret)
        self.fee = fee
        self.symbols_info = self.get_symbols_info()

    def get_symbols_info(self) -> Dict[str, dict]:
        try:
            exchange_info = self.client.get_exchange_info()

            metadata = dict()
            for info in exchange_info["symbols"]:
                if info["status"] != "TRADING":
                    continue
                metadata[info["symbol"]] = dict(
                    baseAsset=info["baseAsset"],
                    quoteAsset=info["quoteAsset"],
                    stepSize=list(
                        filter(
                            lambda x: x["filterType"] == "LOT_SIZE",
                            info["filters"],
                        )
                    )[0]["stepSize"],
                )

            return metadata
        except Exception as e:
            raise e

    def get_symbol_info(self, symbol: str) -> Dict:
        try:
            info = self.client.get_symbol_info(symbol)
            self.symbols_info[info["symbol"]] = dict(
                baseAsset=info["baseAsset"],
                quoteAsset=info["quoteAsset"],
                stepSize=list(
                    filter(
                        lambda x: x["filterType"] == "LOT_SIZE",
                        info["filters"],
                    )
                )[0]["stepSize"],
            )
        except Exception as e:
            raise e

    def _after_fee_volume(self, volume: str) -> tuple:
        total = decimal.Decimal(volume)
        fee_rate = decimal.Decimal(self.fee)

        fee = total * fee_rate
        after_fee_volume = total - fee
        return str(after_fee_volume), str(fee)

    def _convert_volume(self, symbol: str, lastPrice: str, quantity: str) -> str:
        if symbol not in self.symbols_info:
            self.get_symbol_info(symbol)

        step_size = self.symbols_info[symbol]["stepSize"]
        lot_size = step_size.index("1") - 1 if step_size.index("1") - 1 > 0 else 0

        original_volume = decimal.Decimal(quantity) / decimal.Decimal(lastPrice)
        volume = (
            "{:.{}f}".format(original_volume, lot_size)
            if lot_size != 0
            else str(int(original_volume))
        )
        return volume

    def get_price(self, symbol: str) -> str:
        return self.client.get_ticker(symbol=symbol)["lastPrice"]

    def get_all_tickers(self) -> List[Dict[str, str]]:
        return self.client.get_all_tickers()

    def get_historical_klines(
        self,
        symbol: str,
        start: int,
        end: int,
        interval: str,
    ):
        klines = self.client.get_historical_klines_generator(
            symbol,
            INTERVAL[interval],
            timestamp(start),
            timestamp(end) - 1,
            klines_type=HistoricalKlinesType.SPOT,
        )
        return klines
        #
        # data_path = "{}/{}".format(dir, interval)
        #
        # if not os.path.exists(data_path):
        #     os.makedirs(data_path)
        #
        # filename = "{}/{}_{}_{}_{}_spot.{}".format(
        #     data_path, symbol, start, end, interval, format
        # )
        # with open(filename, "w") as file_writer:
        #     if format == "csv":
        #         writer = csv.writer(file_writer)
        #         if add_header:
        #             writer.writerow(
        #                 [
        #                     "t",
        #                     "o",
        #                     "h",
        #                     "l",
        #                     "c",
        #                     "v",
        #                     "T",
        #                     "q",
        #                     "n",
        #                     "V",
        #                     "Q",
        #                     "B",
        #                 ]
        #             )
        #         for kline in klines:
        #             writer.writerow(kline)
        #     else:
        #         for k in klines:
        #             kline = dict(
        #                 t=k[0],
        #                 T=k[6],
        #                 s=symbol,
        #                 i=INTERVAL[interval],
        #                 o=k[1],
        #                 c=k[4],
        #                 h=k[2],
        #                 l=k[3],
        #                 v=k[5],
        #                 n=k[8],
        #                 x=int(time.time() * 1000) > k[6],
        #                 V=k[9],
        #                 q=k[7],
        #                 Q=k[10],
        #             )
        #             file_writer.write(f"{json.dumps(kline)}\n")

    def get_all_symbols(self, suffix: str = None) -> List[str]:
        exchange_info = self.client.get_exchange_info()
        symbols = exchange_info["symbols"]
        if suffix is not None:
            return list(
                map(
                    lambda x: x["symbol"],
                    filter(lambda s: s["symbol"].endswith(suffix), symbols),
                )
            )
        return list(map(lambda x: x["symbol"], symbols))

    def get_order(self, coin: str, orderId: str) -> Dict:
        order = self.client.get_order(symlink=coin, orderId=orderId)
        return order

    def create_order(
        self,
        coin: str,
        volume: str,
        action: str,
        resp_type: str = "FULL",
    ) -> Dict:
        order = self.client.create_order(
            symbol=coin,
            side=action,
            type="MARKET",
            quantity=volume,
            newOrderRespType=resp_type,
        )
        return order

    def buy(self, symbol: str, lastPrice: str, quantity: str) -> tuple:
        volume = self._convert_volume(symbol, quantity, lastPrice)
        order = self.create_order(symbol, volume, action="BUY")
        while order["status"] != "FILLED":
            order = self.get_order(order["symbol"], order["orderId"])
            time.sleep(1)
        after_fee_volume, fee = self._after_fee_volume(order["executedQty"])
        return order["cummulativeQuoteQty"], after_fee_volume, fee

    def sell(self, coin: str, volume: str):
        order = self.create_order(coin, volume, action="SELL")
        while order["status"] != "FILLED":
            order = self.get_order(order["symbol"], order["orderId"])
            time.sleep(1)
        return order["cummulativeQuoteQty"], order["executedQty"]


class BinaceWsClient:
    def __init__(self, api_key: str, api_secret: str):
        self.twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)

    def start(self):
        self.twm.start()

    def join(self):
        self.twm.join()

    def watch_all_kline(
        self, symbols: List[str], callback: CallbackType, interval: str
    ):
        streams = map(lambda x: f"{x.lower()}@kline_{interval}", symbols)
        self.twm.start_multiplex_socket(callback=callback, streams=streams)
