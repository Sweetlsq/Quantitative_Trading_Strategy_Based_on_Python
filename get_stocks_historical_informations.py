import numpy as np
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
import akshare as ak
from tqdm import tqdm
import time
import random

# 获取某个股票历史行情数据
def get_historical_stock_market_data(code, start_date, end_date):
    """
        获取指定股票代码(code)在指定时间范围(start_date, end_date)内的历史行情数据
        返回一个DataFrame，包含日期、股票代码、开盘价、最高价、最低价、收盘价、成交量、市盈率、市净率、股息率、总市值
    """
    # 获取A股历史行情数据，指定股票代码、起始日期、结束日期、后复权等参数
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date,
    adjust="hfq")
    # 判断获取的数据是否为空
    if(stock_zh_a_hist_df.empty):
        return stock_zh_a_hist_df
    # 列名和类型转换
    stock_zh_a_hist_df['股票代码'] = code
    stock_zh_a_hist_df = stock_zh_a_hist_df.loc[:, ['股票代码', '日期', '开盘', '收盘', '最高', '最低', '成交量']]
    stock_zh_a_hist_df.columns = ["code", "trade_date", "open", "close", "high", "low", "volume"]
    stock_zh_a_hist_df['trade_date'] = stock_zh_a_hist_df['trade_date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').date())

    stock_a_indicator_lg_df = ak.stock_a_indicator_lg(symbol=code)
    stock_a_indicator_lg_df = stock_a_indicator_lg_df.loc[:, ["trade_date", "pe", "pb", "dv_ratio", "total_mv"]]

    # 合并分别含有["code", "trade_date", "open", "close", "high", "low", "volume"]和["trade_date", "pe", "pb", "dv_ratio", "total_mv"]信息的两个Dataframe
    merged_stock_df = pd.merge(stock_zh_a_hist_df, stock_a_indicator_lg_df, on='trade_date')

    # 获取起始日期和终止日期
    start_date = merged_stock_df['trade_date'].min()
    end_date = merged_stock_df['trade_date'].max()
    # 使用 date_range() 方法生成起始日期和终止日期之间的所有日期序列，并保存到新的 DataFrame
    date_range_df = pd.DataFrame({'trade_date': pd.date_range(start=start_date, end=end_date, freq='1D')})
    date_range_df['trade_date'] = date_range_df['trade_date'].apply(lambda x: x.to_pydatetime().date())
    # 使用 merge() 方法将原 DataFrame 和新 DataFrame 进行合并，这样就会自动补充缺失的日期值
    merged_stock_df = pd.merge(date_range_df, merged_stock_df, on='trade_date', how='left')

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
            merged_stock_df.loc[i, 'pe'] = temp['pe']
            merged_stock_df.loc[i, 'pb'] = temp['pb']
            merged_stock_df.loc[i, 'dv_ratio'] = temp['dv_ratio']
            merged_stock_df.loc[i, 'total_mv'] = temp['total_mv']
        else:
            temp = merged_stock_df.iloc[i]
    return merged_stock_df
# 保存数据到MongoDB
def save_data(data, collection):
    data['trade_date'] = data['trade_date'].astype(str)
    data = data.to_dict('records')
    collection.insert_many(data)

if __name__ == '__main__':
    while True:
        try:
            # 获取过去2013.01.01——2022.12.31的历史行情数据
            start_date = "20130101"
            end_date = '20221231'
            # 连接MongoDB数据库
            collection = MongoClient('mongodb://127.0.0.1:27017')['quant']['daily']
            # stock.txt存的是所有A股代码
            with open("stock.txt", "r") as file:
                all_stocks = file.read().splitlines()
            # success_codes.txt存储成功保存数据的股票代码
            with open('success_codes.txt', 'r') as f:
                success_code = f.read().splitlines()
            # empty_data_codes.txt存储数据为空的股票代码
            with open('empty_data_codes.txt', 'r') as f:
                empty_data_codes = f.read().splitlines()
            # unknown_fault_codes.txt存储未知错位的代码（最后应将此文件的所有代码成功获取）
            with open('unknown_fault_codes.txt', 'r') as f:
                unknown_fault_codes = f.read().splitlines()
            # 是否为测试获取阶段
            test = False
            if test:
                all_stocks = ["301183", "301357", "831304", "300192"]

            # 循环获取所有股票的历史行情数据
            for code in tqdm(all_stocks):
                # 北交所股票跳过，北交所股票都以("43", "83", "87", "88")开头
                if code.startswith(("43", "83", "87", "88")):
                    continue
                # 若股票成功获取、为空或者有未知错误时，跳过
                if (code in success_code) or (code in empty_data_codes) or (code in unknown_fault_codes):
                    continue
                print(f"{code}正在获取")
                # 记录没有数据的股票代码到文件中(先都放入，最后判断若有数据再删掉)
                with open('unknown_fault_codes.txt', 'a') as f:
                    f.write(code + '\n')
                data = get_historical_stock_market_data(code, start_date, end_date)
                # 判断是否获取不到该股票数据
                if (data.empty):
                    with open('empty_data_codes.txt', 'a') as f:
                        f.write(code + '\n')
                        print(f"{code} 获取不到数据.")
                    continue
                # 保存数据到MongoDB中
                save_data(data, collection)
                # 记录已经成功获取该股票数据
                with open('success_codes.txt', 'a') as f:
                    f.write(code + '\n')
                    print(f"{code} has been added to file.")
                # 将该股票代码从unknown_fault_codes中移除
                with open("unknown_fault_codes.txt", "r") as f:
                    lines = f.readlines()
                if lines:
                    lines.pop()  # 如果a.txt不为空，则删除最后一行
                    with open("unknown_fault_codes.txt", "w") as f:
                        f.writelines(lines)
                # 停顿15-30秒，防止频繁
                time.sleep(random.randint(15, 30))
            break
        except Exception as e:
            print(f"程序出错: {e}")
            # 如果程序出错，则等待一段时间后重新运行
            print(f"等待60秒后重新运行程序...")
            time.sleep(60)