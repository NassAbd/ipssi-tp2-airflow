from datetime import datetime, timedelta
from typing import Any
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from schemas import WeatherData

# Arguments par défaut pour le DAG
default_args = {
    'owner': 'abdallah_nassur',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Configuration des villes pour l'extraction
CITIES = {
    "paris": {"lat": 48.8566, "lon": 2.3522, "display_name": "Paris"},
    "lyon": {"lat": 45.7640, "lon": 4.8357, "display_name": "Lyon"},
    "marseille": {"lat": 43.2965, "lon": 5.3698, "display_name": "Marseille"},
}

def extract_weather_data(lat: float, lon: float, city: str) -> dict:
    """Tâche d'extraction brute : appelle l'API Open-Meteo pour récupérer le JSON brut d'une ville."""
    print(f"Extraction des données météo brutes pour {city} (Lat: {lat}, Lon: {lon}) via l'API...")
    
    # Endpoint MeteoFrance sur Open-Meteo
    url = f"https://api.open-meteo.com/v1/meteofrance?latitude={lat}&longitude={lon}&current=temperature_2m,relative_humidity_2m,weather_code"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        raw_json = response.json()
    except Exception as e:
        print(f"Erreur lors de l'appel API pour la ville {city} : {e}")
        raise
        
    print(f"Données brutes récupérées avec succès pour {city}.")
    return raw_json

def validate_weather_data(city_key: str, display_name: str, ti: Any) -> dict:
    """Tâche de transformation/validation : filtre, valide et structure les données via Pydantic."""
    print(f"Début de la validation et transformation pour {display_name}...")
    
    # Récupération des données brutes de la tâche d'extraction associée via XCom
    raw_data = ti.xcom_pull(task_ids=f"extract_weather_{city_key}")
    if not raw_data:
        raise ValueError(f"Aucune donnée brute récupérée pour {display_name}")
        
    current = raw_data.get("current", {})
    temp = current.get("temperature_2m")
    humidity = current.get("relative_humidity_2m")
    code = current.get("weather_code", 0)
    
    # Correspondance des codes météo WMO
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
    
    # Validation stricte via Pydantic
    weather_info = WeatherData(
        city=display_name,
        temperature=temp,
        conditions=conditions,
        humidity=humidity
    )
    
    print(f"Validation Pydantic réussie pour {display_name}.")
    return weather_info.model_dump()

def save_weather_report(ti: Any) -> None:
    """Tâche finale : agrège les rapports préparés de toutes les villes et les affiche."""
    print("Début de l'agrégation des rapports météo...")
    
    reports = []
    for key in CITIES.keys():
        val_data = ti.xcom_pull(task_ids=f"validate_weather_{key}")
        if val_data:
            reports.append(val_data)
            
    if not reports:
        raise ValueError("Aucun rapport météo validé trouvé")
        
    print(f"--- RAPPORT MÉTÉO CONSOLIDÉ ({len(reports)} VILLES) ---")
    for r in reports:
        print(f"[{r['city']}] Température: {r['temperature']}°C | Humidité: {r['humidity']}% | Conditions: {r['conditions']} | Enregistré à (UTC): {r['timestamp']}")
    print("-------------------------------------------------------")

with DAG(
    'tp2_simple_dag',
    default_args=default_args,
    description='TP2A : Ingestion météo pour plusieurs villes avec validation Pydantic',
    schedule=None,  # Déclenchement uniquement manuel
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['ipssi', 'tp2A'],
) as dag:

    # Définition de la tâche finale de consolidation
    task_save = PythonOperator(
        task_id='save_weather_report',
        python_callable=save_weather_report,
    )

    # Création dynamique des tâches d'extraction et de validation pour chaque ville
    for key, info in CITIES.items():
        task_extract = PythonOperator(
            task_id=f'extract_weather_{key}',
            python_callable=extract_weather_data,
            op_kwargs={
                'lat': info['lat'],
                'lon': info['lon'],
                'city': info['display_name'],
            }
        )

        task_validate = PythonOperator(
            task_id=f'validate_weather_{key}',
            python_callable=validate_weather_data,
            op_kwargs={
                'city_key': key,
                'display_name': info['display_name'],
            }
        )

        # Dépendances explicites : Extract >> Validate >> Save
        task_extract >> task_validate >> task_save
