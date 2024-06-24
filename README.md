# A2_computacao_escalavel, made by
- Almir Fonseca
- Gabriel Pereira
- Gustavo Rocha
- Juliana Carvalho

# How to run on Windows:

- Docker: https://docs.docker.com/desktop/install/windows-install/

Install the Docker and its images and run the docker-compose:

1. Start the docker engine (installed application)
2. ```{bash} docker-compose up --build -d```

To view the Dashboard web app:

- http://localhost:5000/

To check the database state:

- docker exec -it postgres psql -U myuser mydatabase
- then run [do not forget schema and the ;] "SELECT * FROM conta_verde.users;"

Instructions to use AWS [Academy]

- Join the AWS Academy Learner Lab
- Go to modules
- Init the Learning Lab of AWS Academy
- When the AWS Status is green, you can click on it and see the services
