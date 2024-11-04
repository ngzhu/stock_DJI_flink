import yfinance as yf
import pymysql
from datetime import timedelta, datetime

# 配置数据库连接
db_config = {
    'user': 'root',
    'password': '123456',
    'host': 'localhost',
    'port': 3306,
    'database': 'test_stock'
}

# 连接到 MySQL 服务器
connection = pymysql.connect(**db_config)
cursor = connection.cursor()

# 获取数据库中最后一行的时间戳
cursor.execute("SELECT MAX(datetime) FROM dow_jones_data")
last_datetime = cursor.fetchone()[0]

# 如果数据库为空，设置默认开始日期；否则从最后时间戳开始
if last_datetime is None:
    start_date = "2024-10-01"  # 初始数据起始时间
else:
    start_date = (last_datetime + timedelta(minutes=5)).strftime("%Y-%m-%d")  # 去除时间部分，确保符合格式要求

# 设置终止时间为当前时间
end_date = datetime.now().strftime("%Y-%m-%d")

print(start_date, end_date)
# 获取新数据
ticker = "^DJI"
new_df = yf.download(ticker, start=start_date, end=end_date, interval="5m")
print(new_df)