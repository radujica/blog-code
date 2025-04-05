# Setup & Run

    kind create cluster
    helm repo add dagster https://dagster-io.github.io/helm
    helm install my-release dagster/dagster --namespace dagster --create-namespace
