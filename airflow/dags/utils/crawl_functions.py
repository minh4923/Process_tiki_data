import requests
import json
import time
import logging
from bs4 import BeautifulSoup
import re
from typing import List, Dict, Any
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ==================== TAXONOMY FUNCTIONS ====================

def get_root_categories_ids():
    """Lấy danh sách root category IDs từ trang chủ Tiki"""
    url = "https://tiki.vn/"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, "html.parser")
        ids = {int(match.group(1)) for a in soup.find_all("a", href=True) if (match := re.search(r"/c(\d+)", a["href"]))}
        logging.info(f"✅ Found {len(ids)} root categories")
        return sorted(list(ids))
    except Exception as e:
        logging.error(f"❌ Error getting root categories: {e}")
        raise

def crawl_taxonomy_task() -> List[Dict]:
    """Crawl toàn bộ taxonomy của Tiki."""
    logging.info("🚀 Starting taxonomy crawl...")
    result = []
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    def crawl_recursive(parent_id, level=2):
        url = f"https://tiki.vn/api/v2/categories?include=children&parent_id={parent_id}"
        try:
            res = session.get(url, timeout=10)
            res.raise_for_status()
            children = res.json().get("data", [])
            for item in children:
                node = {
                    "id": item["id"], "name": item["name"], "parent_id": item.get("parent_id"),
                    "level": level, "is_leaf": item.get("is_leaf", False), "url_path": item.get("url_path")
                }
                result.append(node)
                if not item.get("is_leaf", False):
                    crawl_recursive(item["id"], level + 1)
                time.sleep(0.2)
        except Exception as e:
            logging.error(f"❌ Error fetching data for category {parent_id}: {e}")
            return

    root_ids = get_root_categories_ids()
    # Giới hạn để test, xóa hoặc comment out dòng này khi chạy thật
    root_ids_to_process = root_ids[:2] 
    logging.info(f"💡 DEBUG MODE: Processing {len(root_ids_to_process)} root categories.")

    for root_id in root_ids_to_process:
        result.append({"id": root_id, "name": f"ROOT_{root_id}", "parent_id": None, "level": 1, "is_leaf": False, "url_path": None})
        crawl_recursive(root_id, level=2)
    logging.info(f"✅ Crawl completed. Found {len(result)} categories.")
    return result

def save_taxonomy_task(taxonomy_data: List[Dict], postgres_conn_id: str) -> int:
    """Lưu taxonomy vào DB, tự động cập nhật nếu đã tồn tại (UPSERT)."""
    if not taxonomy_data:
        logging.info("✅ No taxonomy data to save. Skipping.")
        return 0
    logging.info(f"💾 Starting to save/update {len(taxonomy_data)} categories...")
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    upsert_sql = """
        INSERT INTO dim_category (category_id, name, level, parent_id, url_path, is_leaf)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (category_id) DO UPDATE SET
            name = EXCLUDED.name, level = EXCLUDED.level, parent_id = EXCLUDED.parent_id,
            url_path = EXCLUDED.url_path, is_leaf = EXCLUDED.is_leaf, updated_at = CURRENT_TIMESTAMP;
    """
    rows_to_process = [(cat.get("id"), cat.get("name"), cat.get("level"), cat.get("parent_id"), cat.get("url_path"), cat.get("is_leaf", False)) for cat in taxonomy_data]
    try:
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS dim_category (
                        category_id BIGINT PRIMARY KEY, name TEXT, level INT, parent_id BIGINT, 
                        url_path TEXT, is_leaf BOOLEAN,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cursor.executemany(upsert_sql, rows_to_process)
        logging.info(f"✅ Successfully saved/updated {len(rows_to_process)} categories.")
        return len(rows_to_process)
    except Exception as e:
        logging.error(f"❌ Database error during taxonomy save: {e}")
        raise

# ==================== PRODUCT FUNCTIONS ====================

def get_leaf_categories_batches(taxonomy_data: List[Dict], batch_size: int = 20) -> List[List[Dict]]:
    """Lấy các leaf categories và chia thành các batch (list của các list)."""
    leaf_cats = [cat for cat in taxonomy_data if cat.get("is_leaf")]
    logging.info(f"🔍 Found {len(leaf_cats)} leaf categories. Creating batches of size {batch_size}.")
    batches = [leaf_cats[i:i + batch_size] for i in range(0, len(leaf_cats), batch_size)]
    # Giới hạn để test, xóa hoặc comment out dòng này khi chạy thật
    debug_batches = batches[:2]
    logging.info(f"💡 DEBUG MODE: Processing {len(debug_batches)} product batches.")
    return debug_batches

def simplify_product_data(product: Dict) -> Dict:
    """Đơn giản hóa dữ liệu product."""
    qs = product.get("quantity_sold") or {}
    return {
        "id": product.get("id"), "sku": product.get("sku"), "name": product.get("name"),
        "price": product.get("price"), "original_price": product.get("original_price"),
        "discount_rate": product.get("discount_rate"), "rating_average": product.get("rating_average"),
        "review_count": product.get("review_count"), "quantity_sold": qs.get("value", 0),
        "thumbnail_url": product.get("thumbnail_url"), "url_path": product.get("url_path"),
        "category_id": int(product.get("primary_category_path", "0/0").split("/")[-1])
    }

def crawl_products_batch(category_batch: List[Dict]) -> List[Dict]:
    """Crawl products cho một batch category và trả về list các sản phẩm."""
    logging.info(f"🕷️ Crawling a batch with {len(category_batch)} categories.")
    crawled_products = []
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    for cat in category_batch:
        url = f"https://tiki.vn/api/personalish/v1/blocks/listings?limit=40&category={cat['id']}&page=1"
        try:
            res = requests.get(url, headers=headers, timeout=10)
            res.raise_for_status()
            raw_items = res.json().get("data", [])
            if isinstance(raw_items, list):
                crawled_products.extend([simplify_product_data(p) for p in raw_items])
        except Exception as e:
            logging.warning(f"⚠️ Could not crawl category {cat.get('id', 'N/A')}: {e}")
        time.sleep(0.2)
    logging.info(f"✅ Batch completed. Crawled {len(crawled_products)} products.")
    return crawled_products

def save_products_batch(products_list: List[Dict], postgres_conn_id: str) -> int:
    """Lưu một list sản phẩm vào DB, tự động cập nhật nếu đã tồn tại (UPSERT)."""
    if not products_list:
        logging.info("✅ No products in this batch to save. Skipping.")
        return 0
    logging.info(f"💾 Starting to save/update {len(products_list)} products to DB.")
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    upsert_sql = """
        INSERT INTO products (id, sku, name, price, original_price, discount_rate, rating_average, review_count, quantity_sold, thumbnail_url, url_path, category_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, price = EXCLUDED.price, original_price = EXCLUDED.original_price,
            discount_rate = EXCLUDED.discount_rate, rating_average = EXCLUDED.rating_average,
            review_count = EXCLUDED.review_count, quantity_sold = EXCLUDED.quantity_sold,
            updated_at = CURRENT_TIMESTAMP;
    """
    target_fields = ['id', 'sku', 'name', 'price', 'original_price', 'discount_rate', 'rating_average', 'review_count', 'quantity_sold', 'thumbnail_url', 'url_path', 'category_id']
    rows_to_process = [tuple(p.get(field) for field in target_fields) for p in products_list if p.get('id') is not None]
    try:
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS products (
                        id BIGINT PRIMARY KEY, sku TEXT, name TEXT, price NUMERIC, 
                        original_price NUMERIC, discount_rate NUMERIC, rating_average NUMERIC,
                        review_count INT, quantity_sold INT, thumbnail_url TEXT,
                        url_path TEXT, category_id BIGINT,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cursor.executemany(upsert_sql, rows_to_process)
        logging.info(f"✅ Successfully saved/updated {len(rows_to_process)} products.")
        return len(rows_to_process)
    except Exception as e:
        logging.error(f"❌ Database error during product save: {e}")
        raise