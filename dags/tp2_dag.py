from datetime import datetime, timedelta, timezone
from typing import Any
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param  # type: ignore
from airflow.sdk.exceptions import AirflowSkipException
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

def extract_weather_data(city_key: str, lat: float, lon: float, city: str, **kwargs: Any) -> dict:
    """Tâche d'extraction brute : appelle l'API Open-Meteo pour récupérer le JSON brut d'une ville."""
    # Récupération des paramètres d'exécution
    params = kwargs.get('params', {})
    cities_to_process = params.get('cities', [])
    
    # Gestion du paramétrage : skip si la ville n'est pas sélectionnée
    if city_key not in cities_to_process:
        raise AirflowSkipException(f"La ville {city} ({city_key}) est exclue des paramètres d'ingestion.")
        
    print(f"Extraction des données météo brutes pour {city} (Lat: {lat}, Lon: {lon}) via l'API...")
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

def validate_weather_data(city_key: str, display_name: str, **kwargs: Any) -> dict:
    """Tâche de transformation/validation : filtre, valide et structure les données via Pydantic."""
    print(f"Début de la validation et transformation pour {display_name}...")
    ti = kwargs['ti']
    
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

def load_weather_data(city_key: str, display_name: str, **kwargs: Any) -> int:
    """Tâche de chargement : insère les données météo nettoyées et validées dans PostgreSQL."""
    print(f"Début du chargement SQL pour {display_name}...")
    ti = kwargs['ti']
    
    # Récupération des données validées depuis l'étape de validation via XCom
    validated_data = ti.xcom_pull(task_ids=f"validate_weather_{city_key}")
    if not validated_data:
        print(f"Pas de données validées pour {display_name}.")
        return 0
        
    # Connexion à la base via PostgresHook
    hook = PostgresHook(postgres_conn_id="postgres_default")
    
    sql = """
    INSERT INTO weather_measures (city, temperature, conditions, humidity, timestamp)
    VALUES (%s, %s, %s, %s, %s);
    """
    parameters = (
        validated_data["city"],
        validated_data["temperature"],
        validated_data["conditions"],
        validated_data["humidity"],
        validated_data["timestamp"]
    )
    
    hook.run(sql, parameters=parameters)
    print(f"Données de {display_name} chargées avec succès dans PostgreSQL.")
    return 1

def log_ingestion_run(**kwargs: Any) -> None:
    """Tâche d'audit finale : enregistre l'exécution globale dans la table d'audit."""
    print("Début de l'enregistrement de l'audit d'ingestion...")
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    logical_date = kwargs.get('logical_date') or dag_run.logical_date or datetime.now(timezone.utc)
    
    processed_cities: list[str] = []
    total_inserted = 0
    
    for key in CITIES.keys():
        # Récupération du retour de la tâche de chargement (1 si inséré, sinon None/0)
        inserted = ti.xcom_pull(task_ids=f"load_weather_{key}")
        if inserted == 1:
            processed_cities.append(str(CITIES[key]["display_name"]))
            total_inserted += 1
            
    cities_str = ", ".join(processed_cities) if processed_cities else "None"
    status = "SUCCESS" if total_inserted > 0 else "SKIPPED"
    
    # Connexion et écriture dans la table d'audit
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = """
    INSERT INTO ingestion_runs (run_id, execution_date, status, cities_processed, records_inserted)
    VALUES (%s, %s, %s, %s, %s);
    """
    parameters = (
        dag_run.run_id,
        logical_date,
        status,
        cities_str,
        total_inserted
    )
    
    hook.run(sql, parameters=parameters)
    print(f"Audit d'ingestion enregistré avec succès : Run ID: {dag_run.run_id} | Statut: {status} | Lignes: {total_inserted}")

with DAG(
    'tp2_simple_dag',
    default_args=default_args,
    description='TP2B : Ingestion complète météo vers PostgreSQL avec audit et paramétrage',
    schedule=None,  # Déclenchement uniquement manuel
    start_date=datetime(2026, 6, 1),
    catchup=False,
    params={  # type: ignore
        "cities": Param(
            default=["paris", "lyon", "marseille"],
            type="array",
            description="Liste des clés de villes à ingérer (options: paris, lyon, marseille)",
        )
    },
    tags=['ipssi', 'tp2B'],
) as dag:

    # Définition de la tâche finale d'audit
    task_audit = PythonOperator(
        task_id='log_ingestion_run',
        python_callable=log_ingestion_run,
        trigger_rule='all_done',  # S'exécute même si certaines branches sont skippées
    )

    # Création dynamique des tâches d'extraction, validation et chargement pour chaque ville
    for key, info in CITIES.items():
        task_extract = PythonOperator(
            task_id=f'extract_weather_{key}',
            python_callable=extract_weather_data,
            op_kwargs={
                'city_key': key,
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

        task_load = PythonOperator(
            task_id=f'load_weather_{key}',
            python_callable=load_weather_data,
            op_kwargs={
                'city_key': key,
                'display_name': info['display_name'],
            }
        )

        # Dépendances explicites : Extract >> Validate >> Load >> Audit
        _ = task_extract >> task_validate >> task_load >> task_audit
