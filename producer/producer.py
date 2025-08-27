import pandas as pd, numpy as np
from kafka import KafkaProducer
import time, logging, os, json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER', 'kafka:9092')
DATA_FILE_PATH = os.environ.get('DATA_FILE_PATH', 'producer/data/2022-1217132-one_axis.csv')


def load_data(file_path: str) -> pd.DataFrame:
    """
    Loads the dataset from a specified CSV file path.

    Args:
        file_path (str): The path to the raw CSV data file.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the loaded data.
        Returns an empty DataFrame if loading fails.
    """
    try:
        df = pd.read_csv(file_path, sep=',', on_bad_lines='warn', skiprows=2)
        logging.info("Dataset loaded successfully.")
        return df
    except FileNotFoundError:
        logging.info(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        logging.info(f"An error occurred while loading the data: {e}")
    return pd.DataFrame()


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handles missing values by dropping columns with >50% missing data,
    and then dropping rows with any remaining nulls.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with missing values handled.
    """
    df_copy = df.copy()
    missing_percentage = df_copy.isnull().sum() / len(df_copy)
    cols_to_drop = missing_percentage[missing_percentage > 0.5].index.tolist()

    df_copy.drop(columns=cols_to_drop, inplace=True)
    df_copy.dropna(inplace=True)

    logging.info(f"Dropped {len(cols_to_drop)} columns with >50% missing values.")
    return df_copy


def create_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates and sets a DatetimeIndex from individual time component columns.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with a DatetimeIndex.
    """
    df_copy = df.copy()
    time_cols = ['Year', 'Month', 'Day', 'Hour', 'Minute']
    if all(col in df_copy.columns for col in time_cols):
        df_copy['Datetime'] = pd.to_datetime(df_copy[time_cols])
        # df_copy.set_index('Datetime', inplace=True)
        logging.info("DatetimeIndex created and set successfully.")
    else:
        logging.info("Warning: Not all time columns found; skipping DatetimeIndex creation.")
    return df_copy


def drop_unnecessary_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes columns deemed unnecessary during EDA.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with unnecessary columns removed.
    """
    df_copy = df.copy()
    basic_columns = ['Datetime', 'GHI', 'DNI', 'Temperature']
    other_columns = ['DHI', 'Clearsky DHI', 'Clearsky DNI', 'Clearsky GHI', 'Wind Speed', 'Wind Direction', 'Pressure', 'Cloud Type', 'Relative Humidity', 'Solar Zenith Angle']
    irrelevant_columns = [col for col in df.columns if col not in basic_columns + other_columns]
    df_copy.drop(columns=irrelevant_columns, inplace=True)
    
    logging.info(f"Dropped {len(irrelevant_columns)} unnecessary columns of {len(df.columns)} total columns.")
    return df_copy


def correct_erroneous_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Corrects known erroneous values, such as replacing -9999.0 in
    'Solar Zenith Angle' and forward-filling.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with corrected values.
    """
    df_copy = df.copy()
    if 'Solar Zenith Angle' in df_copy.columns:
        df_copy['Solar Zenith Angle'] = df_copy['Solar Zenith Angle'].replace(-9999.0, np.nan)
        df_copy['Solar Zenith Angle'] = df_copy['Solar Zenith Angle'].ffill()
        logging.info("Corrected erroneous values in 'Solar Zenith Angle'.")
    return df_copy


def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts 'Cloud Type' column to integer type.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with corrected data types.
    """
    df_copy = df.copy()
    if 'Cloud Type' in df_copy.columns:
        df_copy['Cloud Type'] = df_copy['Cloud Type'].astype(int)
        logging.info("Converted 'Cloud Type' to integer.")
    return df_copy


def run_heliosgrid_cleaning_pipeline(file_path: str) -> pd.DataFrame:
    """
    Executes the full data cleaning pipeline by calling all cleaning
    functions in sequence.

    Args:
        file_path (str): The path to the raw CSV data file.

    Returns:
        pd.DataFrame: The cleaned and preprocessed DataFrame.
    """
    logging.info("--- Starting Data Cleaning Pipeline ---")
    
    raw_df = load_data(file_path)
    if raw_df.empty:
        logging.info("Aborting pipeline due to data loading failure.")
        return pd.DataFrame()

    clean_df = (raw_df
        .pipe(handle_missing_values)
        .pipe(create_datetime_index)
        .pipe(drop_unnecessary_columns)
        .pipe(correct_erroneous_values)
        .pipe(convert_data_types)
    )

    logging.info("--- Data Cleaning Pipeline Finished ---\n")
    return clean_df


def send_df_to_kafka(list_of_records: list, producer: KafkaProducer) -> None:
    """
    Sends each record from the list of dictionaries to a Kafka topic.
    
    Args:
        list_of_records (dict): List of dictionaries, each representing a record.
        producer (KafkaProducer): An instance of KafkaProducer.
    """

    if not list_of_records:
        logging.warning("The list of records is empty. No data to send to Kafka.")
        return
    try:
        producer.bootstrap_connected()
    except Exception as e:
        logging.error("Kafka producer is not connected to the broker.", exc_info=True)
        return

    logging.info(f"Starting to send {len(list_of_records)} records to Kafka topic 'solar-power-data'.")
    for record in list_of_records:
        try:
            record['Datetime'] = record['Datetime'].isoformat()
            producer.send('solar-power-data', value=record)
            logging.info(f"Send record: {record}")
            time.sleep(1)
        except Exception as e:
            logging.error(f"Failed to send record: {record}", exc_info=True)
            break
        
    producer.flush()
    logging.info("All messages sent to Kafka topic.")
    producer.close()
    logging.info("Kafka producer closed.")


def main() -> None:
    """Main function to run the data cleaning pipeline and send data to Kafka."""
    if not os.path.isfile(DATA_FILE_PATH):
        logging.info(f"The specified data file path '{DATA_FILE_PATH}' does not exist or is not a file.")
    else:
        cleaned_dataframe = run_heliosgrid_cleaning_pipeline(file_path=DATA_FILE_PATH)
        producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER_URL], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        if not cleaned_dataframe.empty:
            list_of_records = cleaned_dataframe.to_dict(orient='records')
            logging.info(f"\nPrepared {len(list_of_records)} records to send to Kafka.")
            send_df_to_kafka(list_of_records, producer)
            
            # Optional: Save the cleaned DataFrame to a new CSV file
            # OUTPUT_FILE_PATH = 'cleaned_heliosgrid_data.csv'
            # cleaned_dataframe.to_csv(OUTPUT_FILE_PATH)
            # logging.info(f"\nCleaned data saved to '{OUTPUT_FILE_PATH}'")
        else:
            logging.info("No data to send to Kafka after cleaning.")


if __name__ == "__main__":
    main()
else:
    logging.info("This script is intended to be run as the main module.")