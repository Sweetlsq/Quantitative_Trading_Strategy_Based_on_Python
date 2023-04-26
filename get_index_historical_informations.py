import pandas as pd
from datetime import datetime
import tushare as ts
from pymongo import MongoClient

def crawl_index(start_date, end_date, index_codes):
    """
    抓取指数的日K数据。
    指数行情的主要作用：
    1. 用来生成交易日历
    2. 回测时做为收益的对比基准

    :param start_date: 开始日期
    :param end_date: 结束日期
    """

    # 按照指数的代码循环，抓取所有指数信息
    for code in index_codes:
        # 抓取一个指数的在时间区间的数据
        df_daily = ts.get_k_data(code, index=True, start=start_date, end=end_date)
        df_daily.columns = ["trade_date", "open", "close", "high", "low", "volume", "code"]
        df_daily['code'] = code
        # 字符串改为<class 'pandas._libs.tslibs.timestamps.Timestamp'>
        df_daily['trade_date'] = df_daily['trade_date'].apply(
            lambda x: datetime.strptime(x, '%Y-%m-%d'))
        # <class 'pandas._libs.tslibs.timestamps.Timestamp'>列改为datetime.date
        df_daily['trade_date'] = pd.to_datetime(
            df_daily['trade_date']).dt.date

        start = df_daily['trade_date'].min()
        end = df_daily['trade_date'].max()
        # 使用 date_range() 方法生成起始日期和终止日期之间的所有日期序列，并保存到新的 DataFrame
        date_range_df = pd.DataFrame({'trade_date': pd.date_range(start=start, end=end, freq='1D')})
        date_range_df['trade_date'] = date_range_df['trade_date'].apply(lambda x: x.to_pydatetime().date())
        # 使用 merge() 方法将原 DataFrame 和新 DataFrame 进行合并，这样就会自动补充缺失的日期值
        merged_stock_df = pd.merge(date_range_df, df_daily, on='trade_date', how='left')
        # 将非交易日的数据赋值
        temp = merged_stock_df.iloc[0]
        for i in range(merged_stock_df.shape[0]):
            if pd.isna(merged_stock_df.iloc[i]['open']):
                merged_stock_df.loc[i, 'code'] = temp['code']
                merged_stock_df.loc[i, 'open'] = temp['close']
                merged_stock_df.loc[i, 'close'] = temp['close']
                merged_stock_df.loc[i, 'high'] = temp['close']
                merged_stock_df.loc[i, 'low'] = temp['close']
                merged_stock_df.loc[i, 'volume'] = 0
            else:
                temp = merged_stock_df.iloc[i]
        merged_stock_df['trade_date'] = merged_stock_df['trade_date'].astype(str)
        merged_stock_df = merged_stock_df.to_dict('records')
        collection.insert_many(merged_stock_df)
        # 记录已经成功获取该股票数据
        with open('success_codes.txt', 'a') as f:
            f.write(code + '\n')
            print(f"{code} has been added to file.")

if __name__ == '__main__':
    start_date = "20130101"
    end_date = '20221231'
    # 指定抓取的指数列表，可以增加和改变列表里的值
    index_codes = ['000001', '000300', '399001', '399005', '399006', '000905']
    # 连接MongoDB数据库
    collection = MongoClient('mongodb://127.0.0.1:27017')['quant']['daily']
    # get_k_data获取指数行情数据时有bug，起始日期会晚一年
    index = crawl_index("20120101", end_date, index_codes)