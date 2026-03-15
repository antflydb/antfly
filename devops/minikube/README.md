# DevOps Guide for Antfly

This guide explains how to deploy Antfly in a Minikube Kubernetes environment.

## Prerequisites

- Docker installed
- Minikube installed
- kubectl installed
- Go 1.25+ installed

## Building and Deploying

### Local Build

To build the application locally:

```bash
make build
```

This will create an `antfly` binary in the current directory.

### Minikube Deployment

1. Start Minikube:

```bash
make minikube-start
```

2. Build and deploy to Minikube:

```bash
make minikube-deploy
```

This command will:
- Build a Docker image for Antfly
- Push the image to Minikube's Docker registry
- Deploy the metadata and worker nodes to Minikube

3. Check the status of the deployment:

```bash
make minikube-status
```

This will show the status of all pods, services, and deployments.

## Architecture

The Minikube deployment consists of:

1. **metadata Node**: A single pod running with the `--metadata` flag.
2. **Worker Nodes**: Four worker nodes running in a single pod with different IDs and ports:
   - Worker 1: ID 1, Raft port 9021, API port 12380
   - Worker 2: ID 2, Raft port 9022, API port 22380
   - Worker 3: ID 3, Raft port 9023, API port 32380
   - Worker 4: ID 4, Raft port 9024, API port 42380

## Accessing the Services

To access the services from your local machine, you can use port forwarding:

```bash
# Forward metadata API port
kubectl port-forward svc/antfly-metadata-svc 12379:12379

# Forward worker API ports
kubectl port-forward svc/antfly-workers-svc 12380:12380 22380:22380 32380:32380 42380:42380
```

## Cleaning Up

To stop Minikube:

```bash
make minikube-stop
```

To delete the Minikube cluster:

```bash
make minikube-delete
```

## Troubleshooting

If pods are not starting properly, check the logs:

```bash
kubectl logs -l app=antfly,role=metadata
kubectl logs -l app=antfly,role=worker
```

If the Docker image isn't being found, ensure you're building with Minikube's Docker daemon:

```bash
eval $(minikube docker-env)
docker build -t antfly:latest .
```
