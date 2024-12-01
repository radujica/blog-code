# Setup & Run

    kind create cluster
    helm repo add apache-airflow https://airflow.apache.org/
    helm install my-airflow apache-airflow/airflow --version 1.15.0
    kubectl port-forward svc/my-airflow-webserver 8080:8080 --namespace default
