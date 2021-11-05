import datetime  #
import backtrader as bt
import backtrader.feeds as btfeed

if __name__ == "__main__":
    # 创建Cerebro引擎
    cerebro = bt.Cerebro()
    # Cerebro引擎在后台创建broker(经纪人)，系统默认资金量为10000
    data = btfeed.GenericCSVData()

    # 加载交易数据
    cerebro.adddata(data)

    # 设置投资金额100000.0
    cerebro.broker.setcash(100000.0)
    # 引擎运行前打印期出资金
    print("组合期初资金: %.2f" % cerebro.broker.getvalue())
    cerebro.run()
    # 引擎运行后打期末资金
    print("组合期末资金: %.2f" % cerebro.broker.getvalue())
