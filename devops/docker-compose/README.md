# Start
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
