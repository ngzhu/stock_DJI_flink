import yfinance as yf
import pandas as pd
from datetime import datetime
import time

def fetch_and_save_dow_jones_data():
    # 从 Yahoo Finance 获取道琼斯指数数据
    data = yf.download(tickers="^DJI", period="1d", interval="1m")
    latest_data = data.iloc[-1]  # 获取最新一行数据
    print(latest_data)
    
    # # 将数据转换为 DataFrame 格式
    # dow_jones_data = pd.DataFrame({
    #     "timestamp": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
    #     "open": [latest_data["Open"]],
    #     "high": [latest_data["High"]],
    #     "low": [latest_data["Low"]],
    #     "close": [latest_data["Close"]],
    #     "volume": [int(latest_data["Volume"])]
    # })

    # # 生成文件名，带上当前时间戳
    # file_name = f"dow_jones_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    # # 保存为 CSV 文件
    # dow_jones_data.to_csv(file_name, index=False)
    # print(f"Data saved to {file_name}")

# 循环每隔10分钟抓取一次数据并保存
# try:
#     while True:
#         fetch_and_save_dow_jones_data()
#         # time.sleep(600)  # 每 10 分钟运行一次
# except KeyboardInterrupt:
#     print("Data fetching stopped.")
if __name__ == '__main__':
    fetch_and_save_dow_jones_data()