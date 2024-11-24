import json
import psycopg2
import paho.mqtt.client as mqtt

from Functions import username_exists, hash_password, authenticate_user


def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected to MQTT broker.")
    client.subscribe("#")


def on_message(client, userdata, msg, properties=None):
    print("Received a message on topic ", msg.topic)

    if msg.topic == "server/signup":
        json_login_info = json.loads(msg.payload.decode("utf-8"))
        print(f"Received message: {json_login_info}")

        try:
            execute_in_database(
                "INSERT INTO user_data (username, password) VALUES (%s, %s);",
                (json_login_info["username"].lower(), hash_password(json_login_info["password"]))
            )
        except psycopg2.IntegrityError as e:
            if "unique constraint" in str(e):
                returning_data = {
                    "identity": json_login_info["identity"],
                    "user_taken": "True"
                }

                client.publish("application/signup", json.dumps(returning_data))

        except Exception as e:
            print("An error occurred while inserting data:", e)

        else:
            returning_data = {
                "identity": json_login_info["identity"],
                "user_taken": "False"
            }

            client.publish("application/signup", json.dumps(returning_data))

    if msg.topic == "server/login":
        json_login_info = json.loads(msg.payload.decode("utf-8"))
        print(f"Received message: {json_login_info}")

        success = authenticate_user(json_login_info["username"].lower(), json_login_info["password"])
        data_to_return = {
            "identity": json_login_info["identity"],
            "success": success
        }

        client.publish("application/login", json.dumps(data_to_return))

    if msg.topic == "server/create":
        data = json.loads(msg.payload.decode("utf-8"))
        print(f"Received message: {data}")
        username = data["username"].lower()
        recipient_username = data["recipient"].lower()
        chatroom_code = data["code"]
        topic = f"application/chatrooms/{chatroom_code}"

        try:
            execute_in_database(
                "INSERT INTO chatroom_data (chatroom_code) VALUES (%s);",
                (chatroom_code,)
            )
            recipient_available = username_exists(recipient_username)
        except psycopg2.IntegrityError as e:
            if "unique constraint" in str(e):
                data_for_creator = {
                    "username": username,
                    "recipient": recipient_username,
                    "topic": topic,
                    "code_available": "False"
                }

                data_for_recipient = {
                    "username": recipient_username,
                    "recipient": username,
                    "topic": topic,
                    "code_available": "False"
                }

                client.publish("application/create", json.dumps(data_for_creator))
                client.publish("application/create", json.dumps(data_for_recipient))
        else:
            data_for_creator = {
                "username": username,
                "recipient": recipient_username,
                "topic": topic,
                "code_available": "True",
                "recipient_available": recipient_available
            }

            data_for_recipient = {
                "username": recipient_username,
                "recipient": username,
                "topic": topic,
                "code_available": "True",
                "recipient_available": recipient_available
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
    conn.rollback()
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
