from datetime import datetime, timedelta, timezone
from typing import Any
import json
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.sdk import Param  # type: ignore
from airflow.sdk.exceptions import AirflowSkipException
from schemas import WeatherData

# Arguments par défaut pour le DAG conformes aux exigences de production TP5
default_args = {
    'owner': 'abdallah_nassur',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
    'execution_timeout': timedelta(seconds=45),
}

# Configuration des villes pour l'extraction
CITIES = {
    "paris": {"lat": 48.8566, "lon": 2.3522, "display_name": "Paris"},
    "lyon": {"lat": 45.7640, "lon": 4.8357, "display_name": "Lyon"},
    "marseille": {"lat": 43.2965, "lon": 5.3698, "display_name": "Marseille"},
}

def extract_weather_data(city_key: str, lat: float, lon: float, city: str, **kwargs: Any) -> str:
    """Tâche d'extraction brute : appelle l'API Open-Meteo et stocke le JSON brut sur MinIO."""
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
        
    # Simulation d'anomalie de température pour Paris si demandé
    simulate_anomaly = params.get('simulate_anomaly', False)
    if simulate_anomaly and city_key == "paris":
        print("SIMULATION D'ANOMALIE ACTIVE : Injection d'une température aberrante (999.0°C) pour Paris.")
        if "current" in raw_json:
            raw_json["current"]["temperature_2m"] = 999.0
            
    print(f"Données brutes récupérées avec succès pour {city}. Archivage sur MinIO...")
    
    # Détermination de la date logique pour partitionner le stockage brut
    dag_run = kwargs.get('dag_run')
    logical_date = kwargs.get('logical_date')
    if not logical_date and dag_run:
        logical_date = dag_run.logical_date
    if not logical_date:
        logical_date = datetime.now(timezone.utc)
        
    year = logical_date.strftime('%Y')
    month = logical_date.strftime('%m')
    day = logical_date.strftime('%d')
    
    s3_hook = S3Hook(aws_conn_id="minio_conn")
    bucket_name = "weather-raw"
    
    # Création du bucket si absent (gère les accès concurrents lors d'exécutions parallèles)
    try:
        if not s3_hook.check_for_bucket(bucket_name):
            s3_hook.create_bucket(bucket_name)
    except Exception as e:
        if "BucketAlreadyOwnedByYou" in str(e) or "BucketAlreadyExists" in str(e):
            print(f"Le bucket {bucket_name} existe déjà (création en cours/parallèle par un autre worker).")
        else:
            raise
        
    object_key = f"raw/year={year}/month={month}/day={day}/{city_key}_raw.json"
    
    s3_hook.load_string(
        string_data=json.dumps(raw_json),
        key=object_key,
        bucket_name=bucket_name,
        replace=True
    )
    
    print(f"Données brutes stockées avec succès dans MinIO à la clé : {object_key}")
    return object_key

def validate_weather_data(city_key: str, display_name: str, **kwargs: Any) -> dict:
    """Tâche de transformation/validation : lit depuis MinIO, valide et structure les données via Pydantic."""
    print(f"Début de la validation et transformation pour {display_name}...")
    ti = kwargs['ti']
    dag_run = kwargs.get('dag_run')
    logical_date = kwargs.get('logical_date')
    if not logical_date and dag_run:
        logical_date = dag_run.logical_date
    if not logical_date:
        logical_date = datetime.now(timezone.utc)
    
    # Récupération de la clé d'objet MinIO stockée par extract_weather
    object_key = ti.xcom_pull(task_ids=f"extract_weather_{city_key}")
    if not object_key:
        raise ValueError(f"Aucune clé de fichier brut MinIO trouvée pour {display_name}")
        
    # Lecture depuis MinIO
    s3_hook = S3Hook(aws_conn_id="minio_conn")
    bucket_name = "weather-raw"
    
    try:
        raw_data_str = s3_hook.read_key(key=object_key, bucket_name=bucket_name)
        raw_data = json.loads(raw_data_str)
    except Exception as e:
        print(f"Erreur de lecture depuis MinIO pour {object_key} : {e}")
        raise
        
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
    
    # Validation stricte via Pydantic en associant la date logique
    weather_info = WeatherData(
        city=display_name,
        temperature=temp,
        conditions=conditions,
        humidity=humidity,
        timestamp=logical_date
    )
    
    print(f"Validation Pydantic réussie pour {display_name}.")
    return weather_info.model_dump()

def check_quality_data(city_key: str, display_name: str, **kwargs: Any) -> str:
    """Tâche de contrôle qualité : compare les métriques aux seuils définis dans les variables Airflow.
    Décide s'il faut charger (PASS) ou tracer l'anomalie (FAIL).
    """
    ti = kwargs['ti']
    validated_data = ti.xcom_pull(task_ids=f"validate_weather_{city_key}")
    if not validated_data:
        raise ValueError(f"Aucune donnée validée trouvée pour {display_name}")
        
    temp = validated_data["temperature"]
    humidity = validated_data["humidity"]
    
    # Récupération des seuils depuis les Variables Airflow
    try:
        temp_min = float(Variable.get("weather_temp_min", default_var="-50.0"))
        temp_max = float(Variable.get("weather_temp_max", default_var="60.0"))
        humidity_min = int(Variable.get("weather_humidity_min", default_var="0"))
        humidity_max = int(Variable.get("weather_humidity_max", default_var="100"))
    except Exception as e:
        print(f"Impossible de lire les variables Airflow, utilisation des valeurs par défaut. Erreur : {e}")
        temp_min, temp_max = -50.0, 60.0
        humidity_min, humidity_max = 0, 100
        
    reasons = []
    if not (temp_min <= temp <= temp_max):
        reasons.append(f"Temperature {temp}°C hors limites [{temp_min}, {temp_max}]")
    if not (humidity_min <= humidity <= humidity_max):
        reasons.append(f"Humidite {humidity}% hors limites [{humidity_min}, {humidity_max}]")
        
    if reasons:
        reason_str = "; ".join(reasons)
        print(f"QUALITE ECHEC pour {display_name} : {reason_str}")
        # Stocke les détails de l'anomalie dans XCom pour la tâche de traçage
        ti.xcom_push(key="anomaly_reason", value=reason_str)
        ti.xcom_push(key="anomaly_metric", value="temperature" if "Temperature" in reason_str else "humidity")
        ti.xcom_push(key="anomaly_value", value=temp if "Temperature" in reason_str else humidity)
        return f"trace_anomaly_{city_key}"
    else:
        print(f"QUALITE SUCCES pour {display_name} (Temp: {temp}°C, Humidité: {humidity}%)")
        return f"load_weather_{city_key}"

def trace_anomaly_data(city_key: str, display_name: str, **kwargs: Any) -> None:
    """Tâche d'écriture d'anomalie : enregistre les métriques défaillantes dans weather_anomalies."""
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    logical_date = kwargs.get('logical_date') or dag_run.logical_date or datetime.now(timezone.utc)
    
    # Récupération des données d'anomalie stockées par check_quality
    reason = ti.xcom_pull(task_ids=f"check_quality_{city_key}", key="anomaly_reason") or "Qualité non conforme"
    metric = ti.xcom_pull(task_ids=f"check_quality_{city_key}", key="anomaly_metric") or "unknown"
    value = ti.xcom_pull(task_ids=f"check_quality_{city_key}", key="anomaly_value") or 0.0
    
    print(f"Enregistrement de l'anomalie pour {display_name} dans la base de données...")
    
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = """
    INSERT INTO weather_anomalies (city, metric, value, reason, run_id, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    parameters = (
        display_name,
        metric,
        value,
        reason,
        dag_run.run_id,
        logical_date
    )
    hook.run(sql, parameters=parameters)
    print(f"Anomalie pour {display_name} correctement enregistrée dans PostgreSQL.")

def load_weather_data(city_key: str, display_name: str, **kwargs: Any) -> int:
    """Tâche de chargement : insère ou met à jour les données météo dans PostgreSQL (Idempotence)."""
    print(f"Début du chargement SQL pour {display_name}...")
    ti = kwargs['ti']
    
    validated_data = ti.xcom_pull(task_ids=f"validate_weather_{city_key}")
    if not validated_data:
        print(f"Pas de données validées pour {display_name}.")
        return 0
        
    hook = PostgresHook(postgres_conn_id="postgres_default")
    
    # Utilisation de l'Upsert ON CONFLICT pour garantir l'idempotence
    sql = """
    INSERT INTO weather_measures (city, temperature, conditions, humidity, timestamp)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (city, timestamp) 
    DO UPDATE SET 
        temperature = EXCLUDED.temperature,
        conditions = EXCLUDED.conditions,
        humidity = EXCLUDED.humidity;
    """
    parameters = (
        validated_data["city"],
        validated_data["temperature"],
        validated_data["conditions"],
        validated_data["humidity"],
        validated_data["timestamp"]
    )
    
    hook.run(sql, parameters=parameters)
    print(f"Données de {display_name} chargées/mises à jour avec succès dans PostgreSQL.")
    return 1

def log_ingestion_run(**kwargs: Any) -> None:
    """Tâche d'audit finale : enregistre ou met à jour l'exécution dans la table d'audit."""
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
    
    hook = PostgresHook(postgres_conn_id="postgres_default")
    
    # Utilisation de l'Upsert ON CONFLICT pour garantir l'idempotence de l'audit
    sql = """
    INSERT INTO ingestion_runs (run_id, execution_date, status, cities_processed, records_inserted)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (run_id) 
    DO UPDATE SET 
        status = EXCLUDED.status,
        cities_processed = EXCLUDED.cities_processed,
        records_inserted = EXCLUDED.records_inserted,
        inserted_at = CURRENT_TIMESTAMP;
    """
    parameters = (
        dag_run.run_id,
        logical_date,
        status,
        cities_str,
        total_inserted
    )
    
    hook.run(sql, parameters=parameters)
    print(f"Audit d'ingestion enregistré/mis à jour avec succès : Run ID: {dag_run.run_id} | Statut: {status} | Lignes: {total_inserted}")

with DAG(
    'tp2_simple_dag',
    default_args=default_args,
    description='TP5 : Ingestion complète météo industrialisée avec contrôles qualité et idempotence',
    schedule=None,  # Déclenchement uniquement manuel
    start_date=datetime(2026, 6, 1),
    catchup=False,
    params={  # type: ignore
        "cities": Param(
            default=["paris", "lyon", "marseille"],
            type="array",
            description="Liste des clés de villes à ingérer (options: paris, lyon, marseille)",
        ),
        "simulate_anomaly": Param(
            default=False,
            type="boolean",
            description="Simuler une anomalie de température (999.0°C) pour Paris pour tester le branchement qualité",
        )
    },
    tags=['ipssi', 'tp5'],
) as dag:

    # Définition de la tâche finale d'audit
    task_audit = PythonOperator(
        task_id='log_ingestion_run',
        python_callable=log_ingestion_run,
        trigger_rule='all_done',  # S'exécute même si certaines branches sont skippées/anomalies
    )

    # Création dynamique des tâches d'extraction, validation, check, load et trace pour chaque ville
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

        task_check = BranchPythonOperator(
            task_id=f'check_quality_{key}',
            python_callable=check_quality_data,
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

        task_trace = PythonOperator(
            task_id=f'trace_anomaly_{key}',
            python_callable=trace_anomaly_data,
            op_kwargs={
                'city_key': key,
                'display_name': info['display_name'],
            }
        )

        # Dépendances et branchements :
        # extract -> validate -> check_quality -> load OR trace_anomaly -> audit
        _ = task_extract >> task_validate >> task_check
        _ = task_check >> [task_load, task_trace]
        _ = task_load >> task_audit
        _ = task_trace >> task_audit
