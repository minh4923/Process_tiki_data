import psycopg2
# import redis 
import os
from dotenv import load_dotenv

load_dotenv()

def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", 5432),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "123456"),
        database=os.getenv("PG_DATABASE", "tiki_data")
    )
# def get_redis_conn():
#     return redis.Redis(
#         host=os.getenv("REDIS_HOST", "localhost"),
#         port=int(os.getenv("REDIS_PORT", 6379)),
#         db=0,
#         decode_responses=True
#     )