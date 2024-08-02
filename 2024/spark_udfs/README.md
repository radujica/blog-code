# Setup

    conda create -n spark_udfs python pyspark=3.4 openjdk=8


# Run

    JAVA_HOME="$CONDA_PREFIX" conda run --name spark_udfs python run.py
