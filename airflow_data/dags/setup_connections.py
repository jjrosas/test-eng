from airflow.models import Connection
from airflow.settings import Session
import os
import json
from dotenv import load_dotenv

# Cargar variables desde el archivo .env
load_dotenv()

def ensure_connections():
    """Crea o actualiza las conexiones requeridas"""
    session = Session()
    
    # 1. Conexión a MySQL (crea o actualiza)
    mysql_conn = session.query(Connection).filter(Connection.conn_id == 'mysql_employees').first()
    if mysql_conn:
        # Actualizar conexión existente
        mysql_conn.host = os.getenv('MYSQL_HOST', 'localhost')
        mysql_conn.login = os.getenv('DB_USER', 'root')
        mysql_conn.password = os.getenv('DB_PASSWORD', 'password')
        mysql_conn.port = int(os.getenv('MYSQL_PORT', 3306))
        mysql_conn.schema = os.getenv('MYSQL_DB', 'employees')
        print("🔄 Conexión MySQL actualizada")
    else:
        # Crear nueva conexión
        mysql_conn = Connection(
            conn_id='mysql_employees',
            conn_type='mysql',
            host=os.getenv('MYSQL_HOST', 'localhost'),
            login=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', 'password'),
            port=int(os.getenv('MYSQL_PORT', 3306)),
            schema=os.getenv('MYSQL_DB', 'employees')
        )
        session.add(mysql_conn)
        print("✅ Conexión MySQL creada")

    # 2. Conexión a AWS (crea o actualiza)
    aws_conn = session.query(Connection).filter(Connection.conn_id == 'aws_default').first()
    if aws_conn:
        # Actualizar conexión existente
        aws_conn.extra = json.dumps({
            'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'region_name': os.getenv('AWS_REGION', 'us-east-2')
        })
        print("🔄 Conexión AWS actualizada")
    else:
        # Crear nueva conexión
        aws_conn = Connection(
            conn_id='aws_default',
            conn_type='aws',
            extra=json.dumps({
                'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
                'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
                'region_name': os.getenv('AWS_REGION', 'us-east-2')
            })
        )
        session.add(aws_conn)
        print("✅ Conexión AWS creada")

    session.commit()
    session.close()

if __name__ == "__main__":
    ensure_connections()