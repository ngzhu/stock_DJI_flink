import yfinance as yf
import pandas as pd
import pymysql

# 定义道琼斯指数的代码和时间范围
ticker = "^DJI"
start_date = "2024-11-01"
end_date = "2024-11-01"

# 获取数据
df = yf.download(ticker, start=start_date, end=end_date, interval="5m")

# 去除时区信息
df.index = df.index.tz_localize(None)

# 检查数据
print(df.head())

# 保存数据为 Excel 文件
excel_file = "dow_jones_10min_data.xlsx"
df.to_excel(excel_file, engine='openpyxl')
print(f"数据已保存为 {excel_file}")

# 配置数据库连接
db_config = {
    'user': 'root',
    'password': '123456',
    'host': 'localhost',  # 如果是远程服务器，替换为服务器的 IP 地址
    'port': 3306,
}

# 连接到 MySQL 服务器并创建数据库和表
connection = pymysql.connect(**db_config)
cursor = connection.cursor()
database_name = "test_stock"
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
cursor.execute(f"USE {database_name}")

# 创建数据表
table_name = "dow_jones_data"
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    datetime DATETIME PRIMARY KEY,
    open float,
    high float,
    low float,
    close float,
    adj_close float,
    volume BIGINT
)
"""
cursor.execute(create_table_query)

# 插入数据到 MySQL 表
insert_query = f"""
INSERT INTO {table_name} (datetime, open, high, low, close, adj_close, volume)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
for index, row in df.iterrows():
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
print(f"数据已插入到 MySQL 数据库的 `{table_name}` 表中。")

cursor.close()
connection.close()