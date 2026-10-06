# Start
This stack is for local development and runs Antfly without authentication.
Published ports bind to `127.0.0.1`; the listener inside the container remains
on `0.0.0.0` for Docker forwarding. Before exposing the stack remotely, follow
the [secure deployment guide](../../docs/auth.md#secure-deployment), provision a
unique admin password, and configure TLS termination.

To start Antfly with Docker Compose:
```sh
docker compose -f devops/docker-compose/docker-compose.yml up -d --force-recreate
```

# Stop
To stop and remove containers and networks:

```sh
docker compose -f devops/docker-compose/docker-compose.yml down
```

To also remove named volumes:
```sh
docker compose -f devops/docker-compose/docker-compose.yml down --volumes
```

To remove containers, networks, and all images used by the services:
```sh
docker compose -f devops/docker-compose/docker-compose.yml down --rmi all
```
