from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_processed_events_sink_postgres(t_env):
    table_name = 'processed_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime TIMESTAMP(3),
            lpep_dropoff_datetime TIMESTAMP(3),
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance DOUBLE,
            tip_amount DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_events_source_kafka(t_env):
    table_name = "events"
    pattern = "yyyy-MM-dd HH:mm:ss"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            pickup_ts AS TO_TIMESTAMP(lpep_pickup_datetime, '{pattern}'),
            dropoff_ts AS TO_TIMESTAMP(lpep_dropoff_datetime, '{pattern}'),
            WATERMARK FOR dropoff_ts AS dropoff_ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def log_processing():
    # Configuración del entorno de ejecución
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)

    # Configuración del entorno de tabla
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        print("\n🔍 **Creando tablas en Flink...**")
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_processed_events_sink_postgres(t_env)

        print("\n✅ **Tablas creadas exitosamente.**")
        print("📌 Tablas registradas en Flink:", t_env.list_tables())

        print("\n🔍 **Iniciando transferencia de datos de Kafka a PostgreSQL...**")
        t_env.execute_sql(
            f"""
            INSERT INTO {postgres_sink}
            SELECT
                pickup_ts AS lpep_pickup_datetime,
                dropoff_ts AS lpep_dropoff_datetime,
                PULocationID,
                DOLocationID,
                passenger_count,
                trip_distance,
                tip_amount
            FROM {source_table}
            """
        ).wait()

        print("\n✅ **Transferencia completada exitosamente.**")

    except Exception as e:
        print("❌ ERROR: Falló la escritura en PostgreSQL:", str(e))

if __name__ == '__main__':
    log_processing()
