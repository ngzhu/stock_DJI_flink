import yfinance as yf
import pymysql

# 配置数据库连接
db_config = {
    'user': 'root',
    'password': '123456',
    'host': 'localhost',
    'port': 3306,
    'database': 'test_stock'
}

# 定义道琼斯指数的代码和追加的数据时间范围
ticker = "^DJI"
new_start_date = "2024-11-01"
new_end_date = "2024-11-02"

# 获取新数据
new_df = yf.download(ticker, start=new_start_date, end=new_end_date, interval="5m")

print(new_df)
new_df.index = new_df.index.tz_localize(None)  # 去除时区

# 连接到 MySQL 服务器
connection = pymysql.connect(**db_config)
cursor = connection.cursor()

# 定义插入查询语句
insert_query = """
INSERT INTO dow_jones_data (datetime, open, high, low, close, adj_close, volume)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# 遍历新数据，检查是否存在再插入
for index, row in new_df.iterrows():
    # 检查是否已存在该日期时间的数据
    cursor.execute("SELECT COUNT(*) FROM dow_jones_data WHERE datetime = %s", (index,))
    result = cursor.fetchone()
    if result[0] == 0:  # 如果没有找到该日期的数据，则插入
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
