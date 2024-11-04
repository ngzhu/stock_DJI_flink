import yfinance as yf
import pandas as pd

# 定义道琼斯指数的代码
ticker = "^DJI"

# 定义时间范围
start_date = "2024-10-01"
end_date = "2024-10-31"

# 获取数据
df = yf.download(ticker, start=start_date, end=end_date, interval="5m")

# 检查数据
print(df.head())
print(df.dtypes)
# 定义文件名
# excel_file = "dow_jones_10min_data.xlsx"

# # 保存为 Excel 文件
# df.to_excel(excel_file, engine='openpyxl')

# print(f"数据已保存为 {excel_file}")
