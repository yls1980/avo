import requests
from datetime import datetime
import psycopg2

# Airflow and database connection settings
AIRFLOW_BASE_URL = "http://localhost:8080"  # Replace with your Airflow base URL

DATABASE_CONFIG = {
    "host": "your_host",
    "dbname": "your_database",
    "user": "your_user",
    "password": "your_password"
}


# Fetch failed tasks
def get_failed_tasks():
    query = """
        SELECT distinct dag_id, task_id
        FROM task_instance
        WHERE state = 'failed' AND end_date > now()::date
    """
    conn = psycopg2.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()
    cursor.execute(query)
    failed_tasks = cursor.fetchall()
    cursor.close()
    conn.close()
    return failed_tasks


# Restart job by clearing failed tasks
def restart_failed_tasks(failed_tasks):
    for dag_id, task_id in failed_tasks:
        print(f"Restarting task: {task_id} in DAG: {dag_id}")
        url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{dag_id}/dagRuns"

        response = requests.get(
            url,
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
        )

        if response.status_code == 200:
            dag_runs = response.json()
            if dag_runs:
                # Restart only if there are runs
                for run in dag_runs:
                    dag_run_id = run['dag_run_id']
                    print(f"Clearing failed task: {task_id} in DAG Run: {dag_run_id}")
                    clear_url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/clear"
                    clear_response = requests.post(
                        clear_url,
                        auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
                    )

                    if clear_response.status_code == 200:
                        print(f"Task {task_id} in DAG Run {dag_run_id} cleared successfully.")
                    else:
                        print(
                            f"Failed to clear task {task_id} in DAG Run {dag_run_id}. Status: {clear_response.status_code}")
        else:
            print(f"Failed to fetch DAG runs for DAG {dag_id}. Status: {response.status_code}")


# Main function to restart failed tasks
if __name__ == "__main__":
    failed_tasks = get_failed_tasks()
    if failed_tasks:
        restart_failed_tasks(failed_tasks)
    else:
        print("No failed tasks found.")