# Setup

    conda create -n airflow python=3.12
    conda activate airflow
    pip install apache-airflow
    airflow db migrate
    airflow users create -u admin -p admin -f Ad -l Min -r Admin -e admin@gmail.com

# Run

In different terminals:

    airflow scheduler
    airflow webserver

Open localhost:8080

Note that the db init is only finalized for some reason on webserver start.
