"""
Assignment 2 - Kafka Producer
Streaming Data Analytics (SDA-G)

Reads:
    orders_data.csv
    fleet_data.csv

Publishes:
    orders-topic
    fleet-topic

Run from the VS Code terminal after Kafka is running.

Install dependency:
    pip install kafka-python

Run:
    python producer.py
"""

import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

ORDERS_TOPIC = "orders-topic"
FLEET_TOPIC = "fleet-topic"

BASE_DIR = Path(__file__).resolve().parent

ORDERS_FILE = BASE_DIR / "orders_data.csv"
FLEET_FILE = BASE_DIR / "fleet_data.csv"

# Delay between messages.
# 0.25 seconds gives a visible streaming flow in the terminal.
MESSAGE_DELAY_SECONDS = 0.25


# ============================================================
# KAFKA PRODUCER
# ============================================================

def create_producer():
    """Create and return a Kafka producer."""

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(
            value
        ).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8")
        if key is not None
        else None,
        acks="all",
        retries=5,
    )


# ============================================================
# CSV READER
# ============================================================

def read_csv(file_path):
    """Read CSV rows as dictionaries."""

    if not file_path.exists():
        raise FileNotFoundError(
            f"CSV file not found: {file_path}"
        )

    with file_path.open(
        "r",
        newline="",
        encoding="utf-8",
    ) as file:

        reader = csv.DictReader(file)

        for row in reader:
            yield row


# ============================================================
# MESSAGE CLEANING
# ============================================================

def clean_row(row):
    """
    Convert blank CSV values to None.
    Convert numeric-looking values to int/float where appropriate.
    Keep IDs and timestamps as strings.
    """

    cleaned = {}

    integer_fields = {
        "order_items_count",
        "sla_minutes",
        "traffic_index",
        "battery_pct",
        "idle_duration_min",
        "estimated_arrival_min",
        "assigned_order_count",
        "route_deviation_flag",
        "sla_breach_flag",
        "cancellation_flag",
    }

    float_fields = {
        "order_value",
        "distance_km",
        "preparation_time_min",
        "delay_minutes",
        "latitude",
        "longitude",
        "heading_degrees",
        "gps_accuracy_m",
        "speed_kmph",
        "distance_to_destination_km",
        "distance_from_hub_km",
        "temperature_c",
        "rain_mm",
    }

    for key, value in row.items():

        if value is None or value.strip() == "":
            cleaned[key] = None
            continue

        value = value.strip()

        if key in integer_fields:
            try:
                cleaned[key] = int(float(value))
            except ValueError:
                cleaned[key] = value

        elif key in float_fields:
            try:
                cleaned[key] = float(value)
            except ValueError:
                cleaned[key] = value

        else:
            cleaned[key] = value

    return cleaned


# ============================================================
# SEND ORDERS
# ============================================================

def send_orders(producer):
    """Read orders_data.csv and publish records to orders-topic."""

    print("\n" + "=" * 65)
    print("STARTING ORDER STREAM")
    print(f"Topic: {ORDERS_TOPIC}")
    print("=" * 65)

    count = 0

    for row in read_csv(ORDERS_FILE):

        message = clean_row(row)

        order_id = message.get("order_id")

        producer.send(
            ORDERS_TOPIC,
            key=order_id,
            value=message,
        )

        count += 1

        print(
            f"[ORDERS] Message {count:03d} | "
            f"order_id={order_id} | "
            f"rider_id={message.get('rider_id')} | "
            f"status={message.get('delivery_status')} | "
            f"hub={message.get('customer_hub')}"
        )

        # Force messages to be sent to Kafka regularly.
        producer.flush()

        time.sleep(MESSAGE_DELAY_SECONDS)

    print(f"\nOrders published: {count}")


# ============================================================
# SEND FLEET TELEMETRY
# ============================================================

def send_fleet(producer):
    """Read fleet_data.csv and publish records to fleet-topic."""

    print("\n" + "=" * 65)
    print("STARTING FLEET TELEMETRY STREAM")
    print(f"Topic: {FLEET_TOPIC}")
    print("=" * 65)

    count = 0

    for row in read_csv(FLEET_FILE):

        message = clean_row(row)

        rider_id = message.get("rider_id")

        producer.send(
            FLEET_TOPIC,
            key=rider_id,
            value=message,
        )

        count += 1

        print(
            f"[FLEET]  Message {count:04d} | "
            f"rider_id={rider_id} | "
            f"order_id={message.get('order_id')} | "
            f"speed={message.get('speed_kmph')} km/h | "
            f"battery={message.get('battery_pct')}% | "
            f"traffic={message.get('traffic_index')}"
        )

        producer.flush()

        time.sleep(MESSAGE_DELAY_SECONDS)

    print(f"\nFleet messages published: {count}")


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 65)
    print("SDA-G ASSIGNMENT 2 - KAFKA PRODUCER")
    print("=" * 65)

    print(f"Kafka server : {KAFKA_BOOTSTRAP_SERVERS[0]}")
    print(f"Orders file  : {ORDERS_FILE}")
    print(f"Fleet file   : {FLEET_FILE}")

    try:
        producer = create_producer()

        print("\nKafka producer connected successfully.")

        # Stream order events first.
        send_orders(producer)

        # Then stream fleet telemetry.
        send_fleet(producer)

        producer.flush()

        print("\n" + "=" * 65)
        print("ALL DATA SUCCESSFULLY PUBLISHED TO KAFKA")
        print("=" * 65)
        print(f"Orders topic : {ORDERS_TOPIC}")
        print(f"Fleet topic  : {FLEET_TOPIC}")

    except FileNotFoundError as error:
        print(f"\nERROR: {error}")

    except KafkaError as error:
        print("\nKAFKA ERROR:")
        print(error)
        print(
            "\nMake sure Kafka is running and "
            "localhost:9092 is accessible."
        )

    except Exception as error:
        print("\nUNEXPECTED ERROR:")
        print(error)

    finally:
        try:
            producer.close()
        except (NameError, UnboundLocalError):
            pass


if __name__ == "__main__":
    main()
