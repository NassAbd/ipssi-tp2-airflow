from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from schemas import WeatherData

# Argument par défaut pour le DAG
default_args = {
    'owner': 'abdallah_nassur',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def extract_weather_data() -> dict:
    """Appelle l'API MeteoFrance d'Open-Meteo pour extraire les données météo de Paris."""
    print("Début de l'extraction des données météo via l'API Open-Meteo...")
    
    # Coordonnées de Paris
    lat, lon = 48.8566, 2.3522
    url = f"https://api.open-meteo.com/v1/meteofrance?latitude={lat}&longitude={lon}&current=temperature_2m,relative_humidity_2m,weather_code"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Erreur lors de l'appel API : {e}")
        raise
        
    current = data.get("current", {})
    temp = current.get("temperature_2m")
    humidity = current.get("relative_humidity_2m")
    code = current.get("weather_code", 0)
    
    # Correspondance des codes WMO
    wmo_mapping = {
        0: "Clear sky",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Fog",
        48: "Depositing rime fog",
        51: "Drizzle: Light",
        53: "Drizzle: Moderate",
        55: "Drizzle: Dense",
        61: "Rain: Slight",
        63: "Rain: Moderate",
        65: "Rain: Heavy",
        80: "Rain showers: Slight",
        81: "Rain showers: Moderate",
        82: "Rain showers: Violent",
        95: "Thunderstorm",
    }
    
    conditions = wmo_mapping.get(code, f"Unknown code ({code})")
    
    raw_data = {
        "city": "Paris",
        "temperature": temp,
        "conditions": conditions,
        "humidity": humidity
    }
    print(f"Données extraites avec succès depuis l'API : {raw_data}")
    return raw_data

def validate_weather_data(ti) -> dict:
    """Valide les données extraites à l'aide du schéma Pydantic."""
    print("Début de la validation des données...")
    # Récupération des données depuis l'étape d'extraction via XCom
    raw_data = ti.xcom_pull(task_ids="extract_weather_data")
    if not raw_data:
        raise ValueError("Aucune donnée récupérée de la tâche d'extraction")
    
    # Validation par Pydantic
    weather_info = WeatherData(**raw_data)
    print(f"Validation réussie pour la ville : {weather_info.city}")
    return weather_info.model_dump()

def save_weather_report(ti) -> None:
    """Simule l'enregistrement du rapport météo validé."""
    print("Début de la sauvegarde des données...")
    # Récupération des données validées via XCom
    validated_data = ti.xcom_pull(task_ids="validate_weather_data")
    if not validated_data:
        raise ValueError("Aucune donnée validée récupérée")
    
    print("--- RAPPORT MÉTÉO ENREGISTRÉ ---")
    print(f"Ville : {validated_data['city']}")
    print(f"Température : {validated_data['temperature']} °C")
    print(f"Conditions : {validated_data['conditions']}")
    print(f"Humidité : {validated_data['humidity']}%")
    print(f"Date/Heure d'enregistrement : {validated_data['timestamp']}")
    print("---------------------------------")

with DAG(
    'tp2_simple_dag',
    default_args=default_args,
    description='TP2 : Mon premier DAG simple avec 3 tâches',
    schedule=None,  # Pas de planification automatique, uniquement manuel
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['ipssi', 'tp2'],
) as dag:

    task_extract = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data,
    )

    task_validate = PythonOperator(
        task_id='validate_weather_data',
        python_callable=validate_weather_data,
    )

    task_save = PythonOperator(
        task_id='save_weather_report',
        python_callable=save_weather_report,
    )

    # Dépendances explicites
    task_extract >> task_validate >> task_save
