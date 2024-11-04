import yfinance as yf
from kafka import KafkaProducer
import json
from datetime import datetime, timedelta

# Kafka 配置
producer = KafkaProducer(
    bootstrap_servers=['node1:9092','node2:9092','node3:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# 定时抓取数据并发送到 Kafka
def fetch_and_send_data():
    ticker = "^DJI"
    start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    # 获取数据
    data = yf.download(ticker, start=start_date, end=end_date, interval="5m")
    data.index = data.index.tz_localize(None)  # 去除时区
    
    # 清理并发送数据到 Kafka
    for index, row in data.iterrows():
        message = {
            'datetime': index.strftime("%Y-%m-%d %H:%M:%S"),
            'adj_close': row['Adj Close'],
            'close': row['Close'],
            'high': row['High'],
            'low': row['Low'],
            'open': row['Open'],
            'volume': int(row['Volume'])
        }
        producer.send('stock_data', value=message)

fetch_and_send_data()
