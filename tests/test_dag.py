import os
import sys

# S'assurer que le dossier dags est dans le path python pour l'importation
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')))

from airflow.models import DagBag

def test_dag_loading():
    """Vérifie que le DAG s'importe correctement sans erreur et possède la structure TP2A."""
    dagbag = DagBag(dag_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')), include_examples=False)
    
    # Vérification des erreurs d'import
    assert len(dagbag.import_errors) == 0, f"Erreurs d'importation du DAG: {dagbag.import_errors}"
    
    # Vérification que le DAG existe
    dag_id = "tp2_simple_dag"
    assert dag_id in dagbag.dags, f"Le DAG {dag_id} n'a pas été trouvé dans le DagBag"
    
    dag = dagbag.dags[dag_id]
    
    # Pour 3 villes (paris, lyon, marseille) : 3 extractions + 3 validations + 1 sauvegarde = 7 tâches
    assert len(dag.tasks) == 7, f"Le DAG doit avoir exactement 7 tâches pour le multi-villes, trouvé {len(dag.tasks)}"
    
    # Vérification des IDs de tâches attendus
    expected_task_ids = [
        "extract_weather_paris",
        "extract_weather_lyon",
        "extract_weather_marseille",
        "validate_weather_paris",
        "validate_weather_lyon",
        "validate_weather_marseille",
        "save_weather_report"
    ]
    task_ids = [task.task_id for task in dag.tasks]
    assert sorted(task_ids) == sorted(expected_task_ids), f"Tâches attendues {expected_task_ids}, trouvées {task_ids}"
    
    # Vérification des liens de dépendances par ville
    villes = ["paris", "lyon", "marseille"]
    save_task = dag.get_task("save_weather_report")
    
    for ville in villes:
        extract_task = dag.get_task(f"extract_weather_{ville}")
        validate_task = dag.get_task(f"validate_weather_{ville}")
        
        # extract_weather_[ville] >> validate_weather_[ville]
        assert validate_task.task_id in [t.task_id for t in extract_task.downstream_list], f"validate_weather_{ville} doit être en aval de extract_weather_{ville}"
        
        # validate_weather_[ville] >> save_weather_report
        assert save_task.task_id in [t.task_id for t in validate_task.downstream_list], f"save_weather_report doit être en aval de validate_weather_{ville}"
