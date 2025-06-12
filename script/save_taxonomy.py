import json
from config.settings import get_pg_conn

def save_taxonomy():
    with open("data/taxonomy.json", encoding="utf-8") as f:
        data = json.load(f)

    pg_conn = get_pg_conn()
    cursor = pg_conn.cursor()

    # redis_conn = get_redis_conn()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_category (
            category_id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            level INT NOT NULL,
            parent_id BIGINT,
            url_path TEXT,
            is_leaf BOOLEAN DEFAULT FALSE
        );
    """)
    for cat in data:
        cursor.execute("""
            INSERT INTO dim_category (category_id, name, level, parent_id, url_path, is_leaf)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (category_id) DO NOTHING
        """, (
            cat["id"],
            cat["name"],
            cat.get("level"),
            cat.get("parent_id"),
            cat.get("url_path"),
            cat.get("is_leaf", False)
        ))

        # redis_conn.hset(f"category:{cat['id']}", mapping=cat)

    pg_conn.commit()
    cursor.close()
    pg_conn.close()

if __name__ == "__main__":
    save_taxonomy()
