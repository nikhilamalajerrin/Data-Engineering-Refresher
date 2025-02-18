# PostgreSQL and Docker Setup for NY Taxi Data

## Running PostgreSQL
To start a PostgreSQL container:
```sh
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v c:/Users/alexe/git/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

## Running PGCLI
```sh
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

## Running PGAdmin
```sh
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4
```

## Creating a Network for Container Communication
```sh
docker network create pg-network
```

## Running PostgreSQL with a Docker Network
```sh
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v /home/nikhil-james/Workspace/Data-Engineering-Refresher/Week_1/ny_taxi_postgres:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13
```

## Running PGAdmin with a Docker Network
```sh
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin-2 \
  dpage/pgadmin4
```

## Running a Local Python Script
```sh
URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL}
```

## Git Push Workflow
```sh
git add .
git add -- . ':!<path>'
git commit -m "Commit message"
git push -u origin main
```

## Viewing All Containers in Docker
```sh
docker ps -a
docker ps
```

## Running a Script Inside Python
```sh
URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL}
```

## Building a Docker Image for Python Script
```dockerfile
FROM python:3.13

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app

COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python", "ingest_data.py"]
```

### Docker Build & Run
```sh
docker build -t <your_name> .
docker run <your_name> \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL}
```

## Running a Script via Docker
```sh
URL=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}
```

## Docker Compose Setup
```yaml
version: '3.8'

services:
  pgdatabase:
    image: postgres:13
    environment:
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
      - POSTGRES_DB=ny_taxi
    volumes:
      - "/home/nikhil-james/Workspace/Data-Engineering-Refresher/Week_1/ny_taxi_postgres:/var/lib/postgresql/data"
    ports:
      - "5432:5432"
    networks:
      - my_network
  
  pgadmin:
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    ports:
      - "8080:80"
    networks:
      - my_network

networks:
  my_network:
    driver: bridge
```

## Running Docker Compose
```sh
sudo docker compose up  # Runs all containers
docker compose down    # Stops all containers
```

## Note
Instead of using Docker for automating `ingest_data.py`, Apache Airflow would be a better option.





--------------------------------------------------------------------------------
1. Create the postgresql DB from docker and call the instance CONTAINERS
2. Create the network so that the DB can communicate with the ADMIN gui 
3. Create the PGadmin container 
4. Python ingest_data
5. Check the POSTGRES_DB
RUN SQL commands 

---------->

Now we can dockerize this python scipt so that whenever we call the docker container it 
automatically runs and creates the instances 

Create the from method to call python and the run to install whatever needed
Place the py file in pipeline and build the file 

Once built run the build using DOCKER RUN -it with the built name

------------>

Also we can create a new docker compose file so that we can call all containers in
the same time.