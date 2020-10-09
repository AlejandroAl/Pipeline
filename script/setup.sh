#!/usr/bin/env bash


echo "Copying files script_python to src airflow dags"
cp -r ./scripts_python/*.py ./mnt/airflow/dags/src/
cp -r ./scripts_python/dags/*.py ./mnt/airflow/dags/
cp -r ./data/ ./mnt/airflow/dags/src/
echo "Copying completed"