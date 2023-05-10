local-run:
	pyspark < main.py

validate:
	flake8 --config .flake8

docker-test: docker-build
	sudo docker-compose up test

docker-run: docker-build
	sudo docker-compose up app

docker-build:
	sudo docker build . --tag spark-test