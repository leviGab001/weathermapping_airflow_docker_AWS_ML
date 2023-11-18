import os
import json
import pandas as pd

PATH_RAW = '/app/raw_files'
PATH_CLEAN = '/app/clean_data'

def transform_data_into_csv():
    """
    Transforms JSON weather data from raw files into two CSV formats:
    - 'full_data.csv' containing all records in Celsius.
    - 'latest_data.csv' containing the latest 20 records for each city in Celsius.
    """
    parent_folder = PATH_RAW
    files = sorted([f for f in os.listdir(parent_folder) if f.endswith('.json')], reverse=True)

    dfs = []
    for f in files:
        try:
            with open(os.path.join(parent_folder, f), 'r') as file:
                data_temp = json.load(file)
                for data_city in data_temp:
                    temperature_kelvin = data_city['main']['temp']
                    temperature_celsius = temperature_kelvin - 273.15  # Convert from Kelvin to Celsius
                    dfs.append({
                        'temperature': round(temperature_celsius, 2),  # Round to 2 decimal places
                        'city': data_city['name'],
                        'pressure': data_city['main']['pressure'],
                        'date': f.split('.')[0]
                    })
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error processing file {f}: {e}")

    if not dfs:
        print("No data to process.")
        return

    df = pd.DataFrame(dfs)

    # Ensure output directory exists
    os.makedirs(PATH_CLEAN, exist_ok=True)

    # Save all data to full_data.csv
    full_data_path = os.path.join(PATH_CLEAN, 'full_data.csv')
    df.to_csv(full_data_path, index=False)
    print(f"All data saved to {full_data_path}.")

    # Group by city and get the latest 20 records for each city
    df_grouped = df.groupby('city').apply(lambda x: x.sort_values('date', ascending=False).head(20)).reset_index(drop=True)

    # Save the latest 20 records for each city to latest_data.csv
    latest_data_path = os.path.join(PATH_CLEAN, 'latest_data.csv')
    df_grouped.to_csv(latest_data_path, index=False)
    print(f"Latest 20 records for each city saved to {latest_data_path}.")

if __name__ == "__main__":
    transform_data_into_csv()
