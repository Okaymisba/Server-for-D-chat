import psycopg2

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