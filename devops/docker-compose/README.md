# Start
To start Antfly with Docker Compose:
```sh
docker-compose -f devops/docker-compose/docker-compose.yml up -d --force-recreate
```

# Stop
To stop and remove containers and networks:

```sh
docker -f devops/docker-compose/docker-compose.yml compose down
```

To also remove named volumes:
```sh
docker -f devops/docker-compose/docker-compose.yml compose down --volumes
```

To remove containers, networks, and all images used by the services:
```sh
docker -f devops/docker-compose/docker-compose.yml compose down --rmi all
```
