import mysql.connector
from mysql.connector import errorcode

config = {
    'user': 'root',
    'password': 'rootpass',
    'host': 'localhost',
    'database': 'mydb'
}

def main(): 

    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        createUser("misael@test.com", "Misael Armando" "player", "misas1234", "936933980", cursor)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_CANNOT_USER:
            print("❌ Cannot create user:", err)

        elif err.errno == errorcode.ER_DBACCESS_DENIED_ERROR:
            print("❌ Access denied:", err)

        else:
            print("❌ MySQL error:", err)

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def createUser(email: str, name: str, role: str, password: str, phone: str, cursor):

    args = (email, name, phone, None, 33, None)
    cursor.callproc('Criar_utilizador', args)
(
    create_user = f"CREATE USER '{username}'@'%' IDENTIFIED BY '{password}'"
    grant_rote = f"GRANT '{role}' TO '{username}'@'%'"
    set_default = f"SET DEFAULT ROLE '{role}' TO '{username}'@'%'"

    cursor.execute(create_user)
    cursor.execute(grant_rote)
    cursor.execute(set_default)

main()
