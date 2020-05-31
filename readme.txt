1. HDFS
	cd docker-hdfs
	docker build -t hdfs:custom .
	docker run -d -p 8088:8088 -p 9000:9000 -p 50070:50070 -p 50075:50075 -p 50030:50030 -p 50060:50060 -p 50010:50010 --name hdfs_container hdfs:custom
	docker cp <location>/<file name> hdfs_container:/home/<file name>
	docker exec -it hdfs_container bash
	hadoop fs -mkdir /nasa	
	hadoop fs -put /home/<file name> /nasa/<file name>
	hdfs dfs -chmod -R +w /nasa

2. Spark
2.0 Spark base
	cd docker-spark/base
	docker build -t spark-base:custom .

2.1 Spark master
	cd docker-spark/master
	docker build -t spark-master:custom .
	docker run -d -p 8080:8080 -p 7077:7077 --name spark-master_container spark-master:custom
2.2 Spark worker
	cd docker-spark/worker
	!!! In Dockerfile change <spark://spark-master> to your host.docker.internal
	docker build -t spark-worker:custom .
	docker run -d -p 8081:8081 --name spark-worker_container spark-worker:custom
	(If more than one workers needed - modify ports (808x:8081) and container names)	

Running:
	!!! In log_parser.py change <your ip> to your host.docker.internal
	.\spark-submit way\to\script\log_parser.py
