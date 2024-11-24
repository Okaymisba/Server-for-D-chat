import psycopg2
import bcrypt


def hash_password(password):
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')


def username_exists(username):
    try:
        conn = psycopg2.connect(
            dbname="healthsync",
            user="postgres",
            password="12345",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        query = "SELECT 1 FROM user_data WHERE username = %s;"
        cursor.execute(query, (username,))

        result = cursor.fetchone()

        return result is not None
    except Exception as e:
        print(f"Error checking username: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def authenticate_user(username, password):
    try:
        conn = psycopg2.connect(
            dbname="healthsync",
            user="postgres",
            password="12345",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        query = "SELECT password FROM user_data WHERE username=%s;"
        cursor.execute(query, (username,))
        result = cursor.fetchone()

        if result is None:
            return False

        stored_hashed_password = result[0]
        print(stored_hashed_password)

        return bcrypt.checkpw(password.encode('utf-8'), stored_hashed_password.encode('utf-8'))

    except Exception as e:
        print(f"Error during authentication: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
