from utils.db_connector import get_connection

conn = get_connection()

print("CONEXION EXITOSA DESDE PIPELINE")

conn.close()
