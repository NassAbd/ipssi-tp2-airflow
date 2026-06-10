import os
import sys

# S'assurer que le dossier dags est dans le path python pour l'importation
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')))

from airflow.models import DagBag

def test_dag_loading():
    """Vérifie que le DAG s'importe correctement sans erreur et possède la structure TP5 (16 tâches)."""
    dagbag = DagBag(dag_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')), include_examples=False)
    
    # Vérification des erreurs d'import
    assert len(dagbag.import_errors) == 0, f"Erreurs d'importation du DAG: {dagbag.import_errors}"
    
    # Vérification que le DAG existe
    dag_id = "tp2_simple_dag"
    assert dag_id in dagbag.dags, f"Le DAG {dag_id} n'a pas été trouvé dans le DagBag"
    
    dag = dagbag.dags[dag_id]
    
    # 3 extractions + 3 validations + 3 check_quality + 3 chargements + 3 anomalies + 1 log d'audit = 16 tâches
    assert len(dag.tasks) == 16, f"Le DAG doit avoir exactement 16 tâches, trouvé {len(dag.tasks)}"
    
    # Liste des IDs attendus
    expected_task_ids = [
        "extract_weather_paris",
        "extract_weather_lyon",
        "extract_weather_marseille",
        "validate_weather_paris",
        "validate_weather_lyon",
        "validate_weather_marseille",
        "check_quality_paris",
        "check_quality_lyon",
        "check_quality_marseille",
        "load_weather_paris",
        "load_weather_lyon",
        "load_weather_marseille",
        "trace_anomaly_paris",
        "trace_anomaly_lyon",
        "trace_anomaly_marseille",
        "log_ingestion_run"
    ]
    task_ids = [task.task_id for task in dag.tasks]
    assert sorted(task_ids) == sorted(expected_task_ids), f"Tâches attendues {expected_task_ids}, trouvées {task_ids}"
    
    # Vérification des liens de dépendance par ville
    villes = ["paris", "lyon", "marseille"]
    log_task = dag.get_task("log_ingestion_run")
    
    for ville in villes:
        extract_task = dag.get_task(f"extract_weather_{ville}")
        validate_task = dag.get_task(f"validate_weather_{ville}")
        check_task = dag.get_task(f"check_quality_{ville}")
        load_task = dag.get_task(f"load_weather_{ville}")
        trace_task = dag.get_task(f"trace_anomaly_{ville}")
        
        # extract_weather_[ville] >> validate_weather_[ville]
        assert validate_task.task_id in [t.task_id for t in extract_task.downstream_list], f"validate_weather_{ville} doit être en aval de extract_weather_{ville}"
        
        # validate_weather_[ville] >> check_quality_[ville]
        assert check_task.task_id in [t.task_id for t in validate_task.downstream_list], f"check_quality_{ville} doit être en aval de validate_weather_{ville}"
        
        # check_quality_[ville] >> load_weather_[ville] and trace_anomaly_[ville]
        assert load_task.task_id in [t.task_id for t in check_task.downstream_list], f"load_weather_{ville} doit être en aval de check_quality_{ville}"
        assert trace_task.task_id in [t.task_id for t in check_task.downstream_list], f"trace_anomaly_{ville} doit être en aval de check_quality_{ville}"
        
        # load_weather_[ville] >> log_ingestion_run
        assert log_task.task_id in [t.task_id for t in load_task.downstream_list], f"log_ingestion_run doit être en aval de load_weather_{ville}"
        
        # trace_anomaly_[ville] >> log_ingestion_run
        assert log_task.task_id in [t.task_id for t in trace_task.downstream_list], f"log_ingestion_run doit être en aval de trace_anomaly_{ville}"
