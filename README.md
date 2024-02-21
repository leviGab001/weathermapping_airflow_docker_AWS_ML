### Weather Mapping with Airflow, Docker, AWS, and ML

This repository presents a simple solution for weather mapping, integrating Apache Airflow, Docker, AWS services, and Machine Learning (ML) algorithms for predictive analysis.

---

#### Key Features:

- **Data Acquisition:** Fetches weather data from the OpenWeather API for specified cities.
- **Data Transformation:** Processes raw data into structured formats and conducts preprocessing for analysis.
- **Model Training:** Utilizes ML algorithms to train regression models.
- **Model Selection:** Identifies the best-performing model based on evaluation metrics.
- **AWS Integration:** Utilizes AWS services for data storage, including S3 for storing weather data.
- **Dockerized Deployment:** Facilitates easy deployment and scalability through containerization.
- **Apache Airflow Orchestration:** Manages the entire data pipeline with task dependencies and scheduling.

---

#### Requeriments:

- Request Openweather API KEY from https://openweathermap.org/api
- AWS set up for S3

---

#### Setup Instructions:

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/leviGab001/weathermapping_airflow_docker_AWS_ML.git
   ```

2. **Build and Run the Docker Image:**
   ```bash
   cd weathermapping_airflow_docker_AWS_ML
   docker compose up -d --build
   ```

3. **Access Apache Airflow:**
   - Open a web browser and navigate to `http://localhost:8080` to access the Airflow UI.
   - Utilize the provided Airflow DAG to monitor and manage the data pipeline.

4. **Configure AWS:**
   - Set up AWS credentials and ensure appropriate permissions for S3 access.

7. **Set Environment Variables:**
   - Configure environment variables, including `OPENWEATHER_API_KEY` for accessing the OpenWeather API.

8. **Customization:**
   - Customize the DAG, scripts, and Dockerfile to suit specific requirements.
  
