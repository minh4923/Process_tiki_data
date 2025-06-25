import json 
import os
from config.settings import get_pg_conn
def save_products():
    pg_conn = get_pg_conn()
    cur = pg_conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id BIGINT PRIMARY KEY,
            sku TEXT NOT NULL,
            name TEXT NOT NULL,
            price NUMERIC NOT NULL,
            original_price NUMERIC,
            discount_rate NUMERIC,
            rating_average NUMERIC,
            review_count INT,
            quantity_sold INT,
            thumbnail_url TEXT,
            url_path TEXT,
            category_id BIGINT
        );
    """)       
    folder = "data/products"
    for filename in os.listdir(folder):
        if  filename.endswith(".json"):
            path = os.path.join(folder, filename)
            with open(path, encoding = "utf-8") as f:
                try:
                    data = json.load(f)
                    for p in data: 
                        cur.execute("""
                            INSERT INTO products (
                                id, sku, name, price, original_price,
                                discount_rate, rating_average, review_count,
                                quantity_sold, thumbnail_url, url_path, category_id 
                            ) VALUES (
                                %(id)s, %(sku)s, %(name)s, %(price)s, %(original_price)s,
                                %(discount_rate)s, %(rating_average)s, %(review_count)s,
                                %(quantity_sold)s, %(thumbnail_url)s, %(url_path)s, %(category_id)s
                            ) ON CONFLICT (id) DO NOTHING
                    """, p)
                except json.JSONDecodeError as e:
                    print(f"❌ JSON decode error in file {filename}: {e}")
    pg_conn.commit()
    cur.close()
    pg_conn.close()              
if __name__ == "__main__":
    save_products()
    print("✅ Products saved successfully.")               
            

