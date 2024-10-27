import json
import psycopg2
import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected to MQTT broker.")
    client.subscribe("#")


def on_message(client, userdata, msg, properties=None):
    print("Received a message on topic ", msg.topic)

    # creates an entry to the database with username and password
    if msg.topic == "server/login":
        json_login_info = json.loads(msg.payload.decode("utf-8"))
        print(f"Received message: {json_login_info}")

        try:
            execute_in_database(
                "INSERT INTO user_data (username, password) VALUES (%s, %s);",
                (json_login_info["username"], json_login_info["password"])
            )
            print(f"Inserted user: {json_login_info['username']}")
        except Exception as e:
            print("An error occurred while inserting data:", e)

    if msg.topic == "server/create":
        data = json.loads(msg.payload.decode("utf-8"))
        print(f"Received message: {data}")
        username = data["username"]
        recipient_username = data["recipient"]
        chatroom_code = data["code"]
        topic = f"application/chatrooms/{chatroom_code}"

        data_for_creator = {
            "username": username,
            "recipient": recipient_username,
            "topic": topic
        }

        data_for_recipient = {
            "username": recipient_username,
            "recipient" : username,
            "topic" : topic
        }

        client.publish("application/create", json.dumps(data_for_creator))
        client.publish("application/create", json.dumps(data_for_recipient))


def on_connect_for_create_chatroom(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connected to MQTT broker.")
    else:
        print("Failed to connect to MQTT broker.", rc)


def on_message_for_create_chatroom(client, userdata, msg):
    print("Received a message on topic ", msg.topic)


def execute_in_database(query, params=None):
    try:
        conn = psycopg2.connect(
            dbname="healthsync",
            user="postgres",
            password="12345",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
    except Exception as e:
        print("An error occurred while executing query:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


broker = "4dbbebee01cb4916af953cf932ac5313.s1.eu.hivemq.cloud"
port = 8883
username = "Reader"
password = "Reader123"

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
client.username_pw_set(username, password)
client.tls_set()
client.on_connect = on_connect
client.on_message = on_message
client.connect(broker, port)

client.loop_start()

try:
    while True:
        pass
except KeyboardInterrupt:
    print("Exiting...")
finally:
    client.loop_stop()
    client.disconnect()
