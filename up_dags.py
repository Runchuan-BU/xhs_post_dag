import os
import subprocess

def upload_dag(file_path, container_name='airflow-airflow-webserver-1', dest_path='/opt/airflow/dags'):
    """
    Upload a DAG file to the Airflow container.

    :param file_path: Path to the DAG file on the local system
    :param container_name: Name of the Airflow container
    :param dest_path: Destination path in the container
    """
    try:
        command = f"docker cp {file_path} {container_name}:{dest_path}"
        subprocess.run(command, shell=True, check=True)
        print(f"Successfully uploaded {file_path} to {container_name}:{dest_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to upload {file_path} to {container_name}:{dest_path}. Error: {e}")

if __name__ == '__main__':

    local_dag_path = 'xhs_dag.py'

    upload_dag(local_dag_path)
