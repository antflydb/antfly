# Minikube Deployment

Deploys an Antfly cluster to a local Minikube using the manifests in this directory and the published `ghcr.io/antflydb/antfly:latest` image. These manifests are self-hosting examples. Hosted Kubernetes orchestration is maintained in Colony.

## Prerequisites

- Docker
- Minikube (the Makefile uses the `vfkit` driver)
- kubectl

## Deploy

```bash
make minikube-start    # start Minikube, then run minikube-deploy
make minikube-deploy   # pull and load the image, apply these manifests
make minikube-status   # Antfly pods, services, and StatefulSets
```

`minikube-deploy` pulls the published image, tags it `antfly:latest`, loads it into Minikube (the manifests use `imagePullPolicy: Never`), and applies every manifest in this directory.

## Architecture

| Component | Kind | Replicas | Ports |
|-----------|------|----------|-------|
| `antfly-metadata` | StatefulSet + headless `antfly-metadata-svc` | 3 | API 12377, Raft 9017, health 4200 |
| `antfly-workers` | StatefulSet + headless `antfly-workers-svc` | 3 | API 12380, Raft 9021, health 4200 |
| `public-api-svc` | NodePort → metadata API | — | 30002 |

Shared configuration lives in the `antfly-config` ConfigMap.

## Access

```bash
minikube service public-api-svc --url --profile=minikube   # public API URL
kubectl --context=minikube port-forward svc/antfly-metadata-svc 12377:12377
kubectl --context=minikube port-forward svc/antfly-workers-svc 12380:12380
```

## Clean Up

```bash
make minikube-delete
```

## Troubleshooting

```bash
kubectl --context=minikube logs -l app=antfly,role=metadata
kubectl --context=minikube logs -l app=antfly,role=worker
```

If pods report `ErrImageNeverPull`, rerun `make minikube-deploy` to load the image into Minikube.
