# DMRC-Data-Pipeline
Implemented Data Pipeline for Delhi Metro Using Spark Structured Streaming. 
Used Python for IOT Device used at Metro Gates, Kafka for Data Retrieval and Spark for Big Data Processing. 

<h3>Stage 1: IOT Gate</h3>

IOT Device gives invoice of Card ID and charge it takes<br>
![dmrc1](https://user-images.githubusercontent.com/91724657/229353352-5096422c-7711-4b92-83ba-b140d9e29e4d.PNG)

<h3>Stage 2: Kafka Retrives</h3>
Retrives what kafka producer sends
<br>
<img src="https://user-images.githubusercontent.com/91724657/229353704-0ace2bf8-abe6-4eba-a3bd-dbfe8b29b9d4.PNG" width=400>

<h3>Stage 3: Spark in Scala</h3>
It processes by reading from cassandra and updating the database by deducting the charge
<br>
<img src="https://user-images.githubusercontent.com/91724657/229354332-f084c100-6422-48d4-89ae-a0a44d4f1d56.PNG" width=400>

