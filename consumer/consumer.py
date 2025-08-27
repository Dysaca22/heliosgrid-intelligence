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
                temperature FLOAT,
                clearsky_dhi FLOAT,
                clearsky_dni FLOAT,
                clearsky_ghi FLOAT,
                cloud_type INT,
                dhi FLOAT,
                dni FLOAT,
                ghi FLOAT,
                relative_humidity FLOAT,
                solar_zenith_angle FLOAT,
                pressure FLOAT,
                wind_direction FLOAT,
                wind_speed FLOAT
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
                        temperature, clearsky_dhi, clearsky_dni, clearsky_ghi,
                        cloud_type, dhi, dni, ghi, relative_humidity,
                        solar_zenith_angle, pressure, wind_direction, wind_speed
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
            , (
                data.get('Temperature'),
                data.get('Clearsky DHI'),
                data.get('Clearsky DNI'),
                data.get('Clearsky GHI'),
                data.get('Cloud Type'),
                data.get('DHI'),
                data.get('DNI'),
                data.get('GHI'),
                data.get('Relative Humidity'),
                data.get('Solar Zenith Angle'),
                data.get('Pressure'),
                data.get('Wind Direction'),
                data.get('Wind Speed')
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