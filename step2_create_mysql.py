import pymysql

# 配置数据库连接
db_config = {
    'user': 'root',
    'password': '123456',
    'host': 'localhost',  # 如果是远程服务器，替换为服务器的 IP 地址
    'port': 3306
}

# 连接到 MySQL 服务器
connection = pymysql.connect(**db_config)
cursor = connection.cursor()

# 创建数据库
database_name = "your_database_name"
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# 选择数据库
cursor.execute(f"USE {database_name}")

# 创建数据表
table_name = "dow_jones_data"
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    datetime DATETIME PRIMARY KEY,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    adj_close FLOAT,
    volume BIGINT
)
"""
cursor.execute(create_table_query)

# 提交更改并关闭连接
connection.commit()
print(f"数据库 `{database_name}` 和表 `{table_name}` 已成功创建。")

cursor.close()
connection.close()
