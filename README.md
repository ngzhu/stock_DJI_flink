# stock_DJI_flink

# drop the 'data' dow_jones_index from yahoo to kafka 'producer'

# from kafka to flink

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# use pandas caculate the MACD
table = table.add_columns("""
    adj_close.ewm(span=12, adjust=false).mean() AS ema_short,
    adj_close.ewm(span=26, adjust=false).mean() AS ema_long
""").add_columns("""
    (ema_short - ema_long) AS macd,
    (ema_short - ema_long).ewm(span=9, adjust=false).mean() AS signal,
    macd - signal AS histogram
""")
