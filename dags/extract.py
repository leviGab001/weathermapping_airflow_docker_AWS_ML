import requests
import json
import os
import logging
from datetime import datetime

# Setup & Configuration
API_URL = os.environ.get('OPENWEATHER_API_URL', "https://api.openweathermap.org/data/2.5/weather")
API_KEY = os.environ['OPENWEATHER_API_KEY']  # Raises an error if not set
CITIES = ['berlin', 'paris', 'london']
#PATH_RAW = os.environ.get('PATH_RAW', 'raw_files')
PATH_RAW = '/app/raw_files'

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_and_save_weather_data():
    """Fetch weather data for listed cities using OpenWeather API and save to a JSON file."""
    all_data = []

    for city in CITIES:
        params = {
            "q": city,
            "appid": API_KEY
        }
        try:
            response = requests.get(API_URL, params=params)
            response.raise_for_status()
            city_data = response.json()
            if city_data:
                all_data.append(city_data)
        except requests.HTTPError as http_err:
            logging.error(f"HTTP error occurred for {city}: {http_err}")
        except Exception as err:
            logging.error(f"Other error occurred for {city}: {err}")

    now_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    filename = f"{now_time}.json"
    filepath = os.path.join(PATH_RAW, filename)

    try:
        with open(filepath, 'w') as f:
            json.dump(all_data, f, indent=4)
        logging.info(f"Data saved to {filepath}")
    except IOError as e:
        logging.error(f"File IO Error: {e}")

if __name__ == "__main__":
    fetch_and_save_weather_data()
