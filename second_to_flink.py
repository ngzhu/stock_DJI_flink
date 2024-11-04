from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, Kafka, Json

# 设置执行环境
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# 定义 Kafka Source
table_env.connect(
    Kafka()
    .version("universal")
    .topic("stock_data")
    .property("bootstrap.servers", 'node1:9092','node2:9092','node3:9092')
    .start_from_latest()
).with_format(
    Json()
    .fail_on_missing_field(False)
    .schema(DataTypes.ROW([
        DataTypes.FIELD("datetime", DataTypes.STRING()),
        DataTypes.FIELD("adj_close", DataTypes.FLOAT()),
        DataTypes.FIELD("close", DataTypes.FLOAT()),
        DataTypes.FIELD("high", DataTypes.FLOAT()),
        DataTypes.FIELD("low", DataTypes.FLOAT()),
        DataTypes.FIELD("open", DataTypes.FLOAT()),
        DataTypes.FIELD("volume", DataTypes.BIGINT())
    ]))
).with_schema(
    Schema()
    .field("datetime", DataTypes.STRING())
    .field("adj_close", DataTypes.FLOAT())
    .field("close", DataTypes.FLOAT())
    .field("high", DataTypes.FLOAT())
    .field("low", DataTypes.FLOAT())
    .field("open", DataTypes.FLOAT())
    .field("volume", DataTypes.BIGINT())
).create_temporary_table("kafka_source")

# 将数据从 Kafka 表导入 Table
table = table_env.from_path("kafka_source")

# 使用 Table API 来计算 MACD
table = table.add_columns("""
    adj_close.ewm(span=12, adjust=false).mean() AS ema_short,
    adj_close.ewm(span=26, adjust=false).mean() AS ema_long
""").add_columns("""
    (ema_short - ema_long) AS macd,
    (ema_short - ema_long).ewm(span=9, adjust=false).mean() AS signal,
    macd - signal AS histogram
""")

# 输出 MACD 结果
table_env.create_temporary_view("macd_table", table)
result_table = table_env.sql_query("""
    SELECT datetime, adj_close, macd, signal, histogram 
    FROM macd_table
""")

# 定义输出到 Kafka 或其他 Sink (如 MySQL)
# 可以添加一个 Sink，如 MySQL，以存储计算结果

# 执行作业
table_env.execute("Flink MACD Calculation")