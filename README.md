# Real-Time Kafka–Grafana Analytics Pipeline

> A Python-based real-time data streaming pipeline using **Apache Kafka, MySQL, and Grafana** for processing and visualizing streaming operational data.

---

## Overview

This project demonstrates an end-to-end **real-time data streaming and analytics pipeline**.

The system uses **Apache Kafka** as the event streaming layer, Python for data production and consumption, and MySQL for persistent storage. The resulting data can be connected to **Grafana** for real-time operational analytics and dashboarding.

The project demonstrates how continuously generated data can move from a producer through a streaming platform into a database, where it can be queried and visualized for business and operational insights.

---

## Architecture

```text
┌──────────────────────┐
│   Source Data        │
│                      │
│   fleet_data.csv     │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│   Python Producer    │
│                      │
│ ecommerce_producer.py│
└──────────┬───────────┘
           │
           │ Events
           ▼
┌──────────────────────┐
│    Apache Kafka      │
│                      │
│   Kafka Topics       │
└──────────┬───────────┘
           │
           │ Streaming Events
           ▼
┌──────────────────────┐
│   Python Consumer    │
│                      │
│ grafana_consumer.py  │
└──────────┬───────────┘
           │
           │ Processed Data
           ▼
┌──────────────────────┐
│        MySQL         │
│                      │
│ Persistent Storage   │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│       Grafana        │
│                      │
│ Analytics &          │
│ Visualization        │
└──────────────────────┘
