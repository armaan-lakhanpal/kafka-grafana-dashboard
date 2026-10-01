import json
from datetime import datetime

import pymysql
from kafka import KafkaConsumer


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

FLEET_TOPIC = "fleet-topic"
ORDERS_TOPIC = "orders-topic"

MYSQL_HOST = "localhost"
MYSQL_PORT = 3307
MYSQL_USER = "root"
MYSQL_PASSWORD = "Grafana123!"
MYSQL_DATABASE = "sda_g"


# ============================================================
# HELPERS
# ============================================================

def parse_datetime(value):
    if not value:
        return None

    if isinstance(value, datetime):
        return value

    try:
        return datetime.strptime(
            value,
            "%Y-%m-%d %H:%M:%S"
        )
    except (ValueError, TypeError):
        return None


# ============================================================
# MYSQL CONNECTION
# ============================================================

def create_mysql_connection():

    connection = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        autocommit=True
    )

    print("MySQL connected successfully.")

    return connection


# ============================================================
# KAFKA CONNECTION
# ============================================================

def create_consumer():

    return KafkaConsumer(
        FLEET_TOPIC,
        ORDERS_TOPIC,

        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

        # IMPORTANT:
        # Separate group so we can read the existing
        # Kafka messages from the beginning.
        group_id="sda-g-grafana-consumer",

        auto_offset_reset="earliest",

        enable_auto_commit=True,

        value_deserializer=lambda value:
            json.loads(value.decode("utf-8"))
    )


# ============================================================
# ORDER INSERT
# ============================================================

def insert_order(cursor, data):

    sql = """
        INSERT INTO orders_dashboard (
            order_id,
            rider_id,
            customer_hub,
            customer_zone,
            order_value,
            delivery_status,
            order_priority,
            distance_km,
            traffic_index,
            weather_condition,
            turnaround_time_min,
            calculated_delay_min,
            calculated_sla_breach,
            delivery_risk,
            cancellation_flag,
            order_created_at,
            actual_delivery_time,
            promised_delivery_time
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            rider_id = VALUES(rider_id),
            delivery_status = VALUES(delivery_status),
            turnaround_time_min = VALUES(turnaround_time_min),
            calculated_delay_min = VALUES(calculated_delay_min),
            calculated_sla_breach = VALUES(calculated_sla_breach),
            delivery_risk = VALUES(delivery_risk)
    """

    values = (
        data.get("order_id"),
        data.get("rider_id"),
        data.get("customer_hub"),
        data.get("customer_zone"),
        data.get("order_value"),
        data.get("delivery_status"),
        data.get("order_priority"),
        data.get("distance_km"),
        data.get("traffic_index"),
        data.get("weather_condition"),
        data.get("turnaround_time_min"),
        data.get("calculated_delay_min"),
        data.get("calculated_sla_breach"),
        data.get("delivery_risk"),
        data.get("cancellation_flag"),
        parse_datetime(data.get("order_created_at")),
        parse_datetime(data.get("actual_delivery_time")),
        parse_datetime(data.get("promised_delivery_time"))
    )

    cursor.execute(sql, values)


# ============================================================
# FLEET INSERT
# ============================================================

def insert_fleet(cursor, data):

    sql = """
        INSERT IGNORE INTO fleet_dashboard (
            order_id,
            rider_id,
            timestamp,
            speed_kmph,
            battery_pct,
            traffic_index,
            idle_duration_min,
            distance_km,
            temperature_c,
            rain_mm,
            telemetry_anomaly,
            speed_alert,
            idle_alert,
            high_traffic
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
    """

    values = (
        data.get("order_id"),
        data.get("rider_id"),
        parse_datetime(data.get("timestamp")),
        data.get("speed_kmph"),
        data.get("battery_pct"),
        data.get("traffic_index"),
        data.get("idle_duration_min"),
        data.get("distance_km"),
        data.get("temperature_c"),
        data.get("rain_mm"),
        data.get("telemetry_anomaly"),
        data.get("speed_alert"),
        data.get("idle_alert"),
        data.get("high_traffic")
    )

    cursor.execute(sql, values)


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("SDA-G GRAFANA DATA CONSUMER")
    print("=" * 70)

    mysql_connection = None
    consumer = None

    try:

        # ----------------------------------------------------
        # MySQL
        # ----------------------------------------------------

        mysql_connection = create_mysql_connection()

        cursor = mysql_connection.cursor()

        # ----------------------------------------------------
        # Kafka
        # ----------------------------------------------------

        consumer = create_consumer()

        print("Kafka connected successfully.")
        print(
            f"Listening to: {ORDERS_TOPIC}, {FLEET_TOPIC}"
        )
        print("Writing processed data to MySQL...")
        print("-" * 70)

        # ----------------------------------------------------
        # Consume
        # ----------------------------------------------------

        for message in consumer:

            data = message.value

            if message.topic == ORDERS_TOPIC:

                insert_order(cursor, data)

                print(
                    f"[MYSQL ORDER] "
                    f"{data.get('order_id')} | "
                    f"status={data.get('delivery_status')} | "
                    f"risk={data.get('delivery_risk')}"
                )

            elif message.topic == FLEET_TOPIC:

                insert_fleet(cursor, data)

                print(
                    f"[MYSQL FLEET] "
                    f"order={data.get('order_id')} | "
                    f"rider={data.get('rider_id')} | "
                    f"speed={data.get('speed_kmph')} | "
                    f"traffic={data.get('traffic_index')}"
                )

    except KeyboardInterrupt:

        print("\nGrafana consumer stopped.")

    except Exception as error:

        print("\nERROR:")
        print(error)

    finally:

        if consumer:
            consumer.close()

        if mysql_connection:
            mysql_connection.close()

        print("\nConnections closed.")


if __name__ == "__main__":
    main()