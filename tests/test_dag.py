import os
import sys
import pytest

# S'assurer que le dossier dags est dans le path python pour l'importation
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')))

from airflow.models import DagBag

def test_dag_loading():
    """Vérifie que le DAG s'importe correctement sans erreur."""
    # On désactive la vérification stricte du dossier d'exécution pour DagBag
    dagbag = DagBag(dag_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')), include_examples=False)
    
    # Vérification des erreurs d'import
    assert len(dagbag.import_errors) == 0, f"Erreurs d'importation du DAG: {dagbag.import_errors}"
    
    # Vérification que le DAG existe
    dag_id = "tp2_simple_dag"
    assert dag_id in dagbag.dags, f"Le DAG {dag_id} n'a pas été trouvé dans le DagBag"
    
    dag = dagbag.dags[dag_id]
    
    # Vérification qu'il y a exactement 3 tâches
    assert len(dag.tasks) == 3, f"Le DAG doit avoir exactement 3 tâches, trouvé {len(dag.tasks)}"
    
    # Vérification des IDs de tâches
    task_ids = [task.task_id for task in dag.tasks]
    expected_task_ids = ["extract_weather_data", "validate_weather_data", "save_weather_report"]
    assert sorted(task_ids) == sorted(expected_task_ids), f"Tâches attendues {expected_task_ids}, trouvées {task_ids}"
    
    # Vérification des dépendances : extract_weather_data >> validate_weather_data >> save_weather_report
    extract_task = dag.get_task("extract_weather_data")
    validate_task = dag.get_task("validate_weather_data")
    save_task = dag.get_task("save_weather_report")
    
    assert validate_task.task_id in [t.task_id for t in extract_task.downstream_list], "validate_weather_data doit être en aval de extract_weather_data"
    assert save_task.task_id in [t.task_id for t in validate_task.downstream_list], "save_weather_report doit être en aval de validate_weather_data"
