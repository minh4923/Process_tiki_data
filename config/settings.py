import psycopg2
# import redis 
import os
from dotenv import load_dotenv

load_dotenv()
#posgrelocal
# def get_pg_conn():
#     return psycopg2.connect(
#         host=os.getenv("PG_HOST", "localhost"),
#         port=os.getenv("PG_PORT", 5432),
#         user=os.getenv("PG_USER", "postgres"),
#         password=os.getenv("PG_PASSWORD", "123456"),
#         database=os.getenv("PG_DATABASE", "tiki_data")
#     )
# def get_redis_conn():
#     return redis.Redis(
#         host=os.getenv("REDIS_HOST", "localhost"),
#         port=int(os.getenv("REDIS_PORT", 6379)),
#         db=0,
#         decode_responses=True
#     )

#posgre docker
def get_pg_conn():
    return psycopg2.connect(
        host="postgres",  # Use the service name defined in docker-compose
        port=5432,
        user="airflow_db", 
        password="airflow",
        database="airflow"
)