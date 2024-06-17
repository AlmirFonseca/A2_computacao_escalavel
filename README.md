# A2_computacao_escalavel

# Windows:

Install the Docker and its images:

- Docker: https://docs.docker.com/desktop/install/windows-install/
- Image Python (Oficial) Docker: https://hub.docker.com/_/pythons
  - The official is based on debian
- Kafka (Broker): https://hub.docker.com/r/apache/kafka
- PostgreeSQL: https://hub.docker.com/_/postgres

Install PySpark:

- https://spark.apache.org/docs/latest/api/python/getting_started/install.html

Install other dependencies:

pip install -r requirements.txt


To run the mock:

- DOCKER COMPOSE BUILD

To check the database state:

- docker exec -it postgres psql -U myuser mydatabase
- then run [do not forget schema and the ;] "SELECT * FROM conta_verde.users;"
