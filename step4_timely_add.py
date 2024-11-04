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

# 获取新数据
ticker = "^DJI"
new_df = yf.download(ticker, start=start_date, end=end_date, interval="5m")
new_df.index = new_df.index.tz_localize(None)  # 去除时区

# 过滤掉已存在的日期时间数据
new_df = new_df[~new_df.index.isin([last_datetime])]

print(new_df)

# 检查是否有新数据
if new_df.empty:
    print("没有新的数据需要追加。")
else:
    # 定义插入查询语句
    insert_query = """
    INSERT INTO dow_jones_data (datetime, open, high, low, close, adj_close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    # 插入新数据
    for index, row in new_df.iterrows():
        # 避免重复插入
        if index > last_datetime:
            cursor.execute(insert_query, (
                index,
                float(row['Open']),
                float(row['High']),
                float(row['Low']),
                float(row['Close']),
                float(row['Adj Close']),
                int(row['Volume'])
            ))

    # 提交事务并关闭连接
    connection.commit()
    print("新数据已追加到 MySQL 数据库的 `dow_jones_data` 表中。")

cursor.close()
connection.close()