Real-Time Traffic Congestion Analytics System
End-to-end real-time analytics pipeline using Kafka, Spark Structured Streaming, MongoDB, and Power BI.

What's included
Kafka + Zookeeper + MongoDB Docker Compose
Kafka producer (simulated sensors)
Spark Structured Streaming job
FastAPI service to serve aggregates
Power BI instructions for building a live dashboard
Databricks notebook version of the Spark job (in /databricks)
Quickstart (local)
Start services: docker-compose up -d
Create Kafka topic traffic_stream (or let producer auto-create)
Start producer: python3 producer/sensor_producer.py
Run Spark job: spark-submit spark/traffic_streaming.py
Start API: uvicorn api.app:app --reload --host 0.0.0.0 --port 8000
Configure Power BI using dashboard/powerbi_instructions.md
License
MIT — see LICENSE file.
