import os, json, logging, time, psycopg2
from kafka import KafkaConsumer


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'airflow')
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'solar-power-data')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'airflow')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'airflow')


def connect_to_postgres() -> psycopg2.extensions.connection | None:
    """Establish a connection to the PostgreSQL database with retries."""
    conn = None
    retries = 5
    while retries > 0 and conn is None:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            logging.info("Connected to PostgreSQL database")
        except psycopg2.OperationalError as e:
            logging.warning(f"Failed to connect to PostgreSQL, retries left {retries-1}: {e}")
            retries -= 1
            time.sleep(5)
    return conn


def main() -> None:
    """Main function to consume messages from Kafka and store them in PostgreSQL."""
    conn = connect_to_postgres()
    if not conn:
        logging.critical("Could not connect to PostgreSQL database. Exiting.")
        return
    
    cursor = conn.cursor()

    # Example data: {'Temperature': 30.4, 'Clearsky DHI': 118.0, 'Clearsky DNI': 863.0, 'Clearsky GHI': 724.0, 'Cloud Type': 0, 'DHI': 118.0, 'DNI': 863.0, 'GHI': 724.0, 'Relative Humidity': 54.61, 'Solar Zenith Angle': 45.36, 'Pressure': 1012.0, 'Wind Direction': 46.0, 'Wind Speed': 3.9}
    cursor.execute(
        """
            CREATE TABLE IF NOT EXISTS solar_data (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ghi FLOAT,
                dni FLOAT,
                temperature FLOAT,
                dhi FLOAT,
                clearsky_dhi FLOAT,
                clearsky_dni FLOAT,
                clearsky_ghi FLOAT,
                wind_speed FLOAT,
                wind_direction FLOAT,
                pressure FLOAT,
                cloud_type INT,
                relative_humidity FLOAT,
                solar_zenith_angle FLOAT
            );
        """
    )

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER_URL],
        auto_offset_reset='earliest',
        group_id='solar-data-postgres-consumer',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    logging.info(f"Consuming messages from topic: {KAFKA_TOPIC}")

    for message in consumer:
        data = message.value
        logging.info(f"Received message: {data}")

        try:
            cursor.execute(
                """
                    INSERT INTO solar_data (
                        timestamp,
                        ghi,
                        dni,
                        temperature,
                        dhi,
                        clearsky_dhi,
                        clearsky_dni,
                        clearsky_ghi,
                        wind_speed,
                        wind_direction,
                        pressure,
                        cloud_type,
                        relative_humidity,
                        solar_zenith_angle
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
            , (
                data.get('Datetime', None),
                data.get('GHI', None),
                data.get('DNI', None),
                data.get('Temperature', None),
                data.get('DHI', None),
                data.get('Clearsky DHI', None),
                data.get('Clearsky DNI', None),
                data.get('Clearsky GHI', None),
                data.get('Wind Speed', None),
                data.get('Wind Direction', None),
                data.get('Pressure', None),
                data.get('Cloud Type', None),
                data.get('Relative Humidity', None),
                data.get('Solar Zenith Angle', None)
            ))
            conn.commit()
            logging.info("Data inserted successfully into PostgreSQL")
        except Exception as e:
            logging.error(f"Error inserting data into PostgreSQL: {e}")
            conn.rollback() 


if __name__ == "__main__":
    main()
else:
    logging.info("This script is intended to be run as the main module.")