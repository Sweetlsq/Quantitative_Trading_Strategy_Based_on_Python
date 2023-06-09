from pymongo import MongoClient, ASCENDING, DESCENDING
import pandas as pd
import matplotlib.pyplot as plt
import time
import datetime
import pandas_market_calendars as mcal
from tqdm import tqdm
import numpy as np
import pyecharts.options as opts
from pyecharts.charts import Line


# 创建索引，加快查找速度
# collection.create_index([("code", 1)])
# collection.create_index([("trade_date", ASCENDING), ("pe", ASCENDING)])
# collection.create_index([("trade_date", ASCENDING), ('code', ASCENDING), ("volume", ASCENDING)])


# start_time = time.time()    # 记录开始时间
# print("timing....")
# end_time = time.time()      # 记录结束时间
# print('Elapsed time: {:.6f}s'.format(end_time - start_time))    # 输出耗时
# count = collection.count_documents({'code': '000004'})

def get_stocks_pool(start_date, end_date):
    """
    股票池的选股逻辑

    :param begin_date: 开始日期
    :param end_date: 结束日期
    :return: tuple，所有调整日，以及调整日和代码列表对应的dict
    """

    # 返回值：所有的调整日列表
    all_adjust_dates = []
    # 返回值：调整日和当期股票代码列表
    adjust_date_codes_dict = dict()

    # 获取从开始日期到结束日期之间所有A股开市的日期
    china_calendar = mcal.get_calendar('SSE')
    is_trading_day = china_calendar.valid_days(start_date=start_date, end_date=end_date)
    # 将日期转为字符串格式
    is_trading_day = list(map(lambda x: x.date().strftime('%Y-%m-%d'), is_trading_day))

    # 上一期的所有股票代码
    last_phase_codes = []

    # 在调整日调整股票池
    for _index in range(len(is_trading_day)):
        # 保存调整日
        adjust_date = is_trading_day[_index]
        all_adjust_dates.append(adjust_date)
        # print(f'调整日期： {adjust_date}', flush=True)

        # 查询出调整当日，pe满足pe_range要求，且非停牌的股票
        # 最重要的一点是，按照pe正序排列，只取前pool_size只
        daily_cursor = collection.find(
            {'trade_date': adjust_date, 'pe': {'$lt': pe_range[1], '$gt': pe_range[0]},
             'volume': {'$ne': 0}},
            sort=[('pe', sort)],
            projection={'code': True},
            limit=pool_size
        )
        # 拿到所有的股票代码
        codes = [x['code'] for x in daily_cursor]
        # 本期股票列表
        this_phase_codes = []

        # 如果上期股票代码列表不为空，则查询出上次股票池中正在停牌的股票
        if len(last_phase_codes) > 0:
            suspension_cursor = collection.find(
                # 查询是股票代码、日期和是否为交易，这里is_trading=False
                {'code': {'$in': last_phase_codes}, 'trade_date': adjust_date, 'volume': {'$ne': 0}},
                # 只需要使用股票代码
                projection={'code': True}
            )
            # 拿到股票代码
            suspension_codes = [x['code'] for x in suspension_cursor]

            # 保留股票池中正在停牌的股票
            this_phase_codes = suspension_codes

        # 打印出所有停牌的股票代码
        # print('上期停牌', flush=True)
        # print(this_phase_codes, flush=True)

        # 用新的股票将剩余位置补齐
        this_phase_codes += codes[0: pool_size - len(this_phase_codes)]
        # 将本次股票设为下次运行的时的上次股票池
        last_phase_codes = this_phase_codes

        # 建立该调整日和股票列表的对应关系
        adjust_date_codes_dict[adjust_date] = this_phase_codes

        # print('最终出票', flush=True)
        # print(this_phase_codes, flush=True)

        # 计算下adjust_interval个交易日
        _index += adjust_interval
        if (_index >= len(is_trading_day)):
            break

    # 返回结果
    return all_adjust_dates, adjust_date_codes_dict


def statistic_stock_pool(start_date, end_date):
    """
    统计股票池的收益
    """

    # 找到指定时间范围的股票池数据，这里的时间范围可以改变
    adjust_dates, codes_dict = get_stocks_pool(start_date, end_date)

    # 用DataFrame保存收益，profit是股票池的收益，hs300是用来对比的沪深300的涨跌幅
    df_profit = pd.DataFrame(columns=['profit', 'hs300'])

    # 统计开始的第一天，股票池的收益和沪深300的涨跌幅都是0
    df_profit.loc[adjust_dates[0]] = {'profit': 0, 'hs300': 0}

    # 找到沪深300第一天的值，后面的累计涨跌幅都是和它比较
    hs300_begin_value = collection.find_one({'trade_date': adjust_dates[0], 'code': '000300'})['close']

    """
    通过净值的方式计算累计收益：
    累计收益 = 期末净值 - 1
    第N期净值的计算方法：
    net_value(n) = net_value(n-1) + net_value(n-1) * profit(n)
                 = net_value(n-1) * (1 + profit(n))
    """
    # 设定初始净值为1
    net_value = 1
    # 在所有调整日上统计收益，循环时从1开始，因为每次计算要用到当期和上期

    for _index in tqdm(range(1, len(adjust_dates) - 1)):
        # 上一期的调整日
        last_adjust_date = adjust_dates[_index - 1]
        # 当期的调整日
        current_adjust_date = adjust_dates[_index]
        # 上一期的股票代码列表
        codes = codes_dict[last_adjust_date]

        # 构建股票代码和后复权买入价格的股票
        buy_daily_cursor = collection.find(
            {'trade_date': last_adjust_date, 'code': {'$in': codes}},
            projection={'close': True, 'code': True}
        )
        code_buy_close_dict = dict([(buy_daily['code'], buy_daily['close']) for buy_daily in buy_daily_cursor])

        # 找到上期股票的在当前调整日时的收盘价
        # 1. 这里用的是后复权的价格，保持价格的连续性
        # 2. 当前的调整日，也就是上期的结束日
        sell_daily_cursor = collection.find(
            {'trade_date': current_adjust_date, 'code': {'$in': codes}},
            # 只需要用到收盘价来计算收益
            projection={'close': True, 'code': True}
        )

        # 初始化所有股票的收益之和
        profit_sum = 0
        # 参与收益统计的股票数量
        count = 0
        # 循环累加所有股票的收益
        for sell_daily in sell_daily_cursor:
            # 股票代码
            code = sell_daily['code']

            # 选入股票池时的价格
            buy_close = code_buy_close_dict[code]
            # 当前的价格
            sell_close = sell_daily['close']
            # 累加所有股票的收益
            profit_sum += (sell_close - buy_close) / buy_close

            # 参与收益计算的股票数加1
            count += 1

        # 计算平均收益
        profit = round(profit_sum / count, 4)

        # 当前沪深300的值
        hs300_close = collection.find_one({'trade_date': current_adjust_date, 'code': '000300'})['close']

        # 计算净值和累积收益，放到DataFrame中
        net_value = net_value * (1 + profit)
        df_profit.loc[current_adjust_date] = {
            # 乘以100，改为百分比
            'profit': round((net_value - 1) * 100, 4),
            # 乘以100，改为百分比
            'hs300': round((hs300_close - hs300_begin_value) * 100 / hs300_begin_value, 4)}

    """
    用pyecharts画出策略最终收益曲线
    """

    # 设置副标题
    fuhao = ''
    subtitle = f'{pe_range[0]}<pe<{pe_range[1]}\n' \
               f'stock_number:5\n' \
               f'年化收益率：{(pow(net_value, 1 / (int(end_date[:4]) - int(start_date[:4]) + 1)) - 1) * 100:.2f}%\n'

    # 创建图表对象并设置图表基础属性
    c = (
        Line(
            # 添加图表基础设置
            init_opts=opts.InitOpts(
                width="1500px",
                height="600px",
                page_title='Historical Yield',
                bg_color='white'
            )
        )
            # 添加横坐标数据
            .add_xaxis(df_profit.index.values)
            # 添加第一个纵坐标数据系列，名称为 "hs300"
            .add_yaxis(
            "hs300",  # 数据系列的名称
            df_profit['hs300'],  # 数据值
            symbol='circle',  # 设置标记符号为圆
            symbol_size=4,  # 设置标记大小为4
            label_opts=opts.LabelOpts(is_show=False),  # 不显示标签（不在整张图表上显示数值）
            itemstyle_opts=opts.ItemStyleOpts(
                color='black',  # 设置数据项颜色为黑色
            ),

        )
            # 添加第二个纵坐标数据系列，名称为 "profit"
            .add_yaxis(
            "profit",
            df_profit['profit'],  # 数据值
            symbol='circle',  # 设置标记符号为圆
            symbol_size=4,  # 设置标记大小为4
            label_opts=opts.LabelOpts(is_show=False),  # 不显示标签（不在整张图表上显示数值）
            itemstyle_opts=opts.ItemStyleOpts(
                color='red',  # 设置数据项颜色为红色
            ),
        )
            # 设置图表全局属性
            .set_global_opts(
            # 标题配置项
            title_opts=opts.TitleOpts(
                title="Historical Yield",  # 设置标题
                subtitle=subtitle,  # 设置副标题
            ),
            # 区域缩放选项
            # datazoom_opts=opts.DataZoomOpts(
            #     is_show=True,  # 显示区域缩放
            #     type_='slider',  # 设置区域缩放形式为滑动条
            #     is_realtime=True,  # 支持实时缩放
            #     # range_start=0,  # 区域缩放的起始值
            #     # range_end=100,  # 区域缩放的结束值
            #     orient="horizontal",  # 设置方向为水平方向
            #     is_zoom_lock=False,  # 支持同时缩放
            #     start_value=datetime.datetime.strptime(df_profit.index.values[0], '%Y-%m-%d'),  # 设置数据缩放组件的起始时间
            #     end_value=datetime.datetime.strptime(df_profit.index.values[-1], '%Y-%m-%d')  # 设置数据缩放组件的结束时间
            # ),
            # 提示框配置项
            tooltip_opts=opts.TooltipOpts(
                is_show=True,  # 显示提示框
                trigger="axis",  # 设置触发方式为坐标轴触发
                trigger_on='mousemove|click',  # 设置触发方式为鼠标移动和点击触发
                is_show_content=True,  # 显示提示框内容
            ),
            # 横坐标配置项
            xaxis_opts=opts.AxisOpts(
                is_show=True,  # 显示横坐标
                type_="time",  # 设置坐标轴类型为时间类型
                # min_= datetime.datetime.strptime(start_date, '%Y%m%d'),  # 设置 x 轴的最小值
            ),
            # 纵坐标配置项
            yaxis_opts=opts.AxisOpts(
                is_show=True,  # 显示纵坐标
                axisline_opts=opts.AxisLineOpts(is_show=True),  # 显示坐标轴线条
                axistick_opts=opts.AxisTickOpts(is_show=True),  # 显示坐标轴刻度线
            ),
        )
            .render("Historical Yield.html")
    )
    # print(df_profit.index.values[0])
    # print(df_profit.index.values[-1])
    # print(type(df_profit.index.values[0]))
    # print(df_profit.index.values)

if __name__ == '__main__':
    # 创建一个MongoClient对象并连接到本地MongoDB的quant数据库的daily集合
    collection = MongoClient('mongodb://127.0.0.1:27017')['quant']['daily']
    """
    下面的几个参数可以自己修改
    """
    start_date = "20130101"
    end_date = '20221231'
    # 调整周期是22个交易日，可以改变的参数
    adjust_interval = 22
    # 设定PE的范围
    pe_range = (0, 10)
    # PE的排序方式， ASCENDING - 从小到大，DESCENDING - 从大到小
    sort = ASCENDING
    # 股票池内的股票数量
    pool_size = 5

    statistic_stock_pool(start_date, end_date)

