import os
import json
import requests
import time

def simplify_product_data(product):
    qs = product.get("quantity_sold") or {}
    return {
        "id": product.get("id"),
        "sku": product.get("sku"),
        "name": product.get("name"),
        "price": product.get("price"),
        "original_price": product.get("original_price"),
        "discount_rate": product.get("discount_rate"),
        "rating_average": product.get("rating_average"),
        "review_count": product.get("review_count"),
        "quantity_sold": qs.get("value", 0),
        "thumbnail_url": product.get("thumbnail_url"),
        "url_path": product.get("url_path"),
        "category_id": product.get("primary_category_path", "").split("/")[-1]
    }

    
def crawl_products_by_leaf_category(limit=10):
    # Đọc taxonomy đã crawl
    with open("data/taxonomy.json", encoding="utf-8") as f:
        taxonomy = json.load(f)

    # Lọc các danh mục là lá (không có con)
    leaf_cats = [cat for cat in taxonomy if cat.get("is_leaf")]
    print(f"🔍 Tổng số leaf categories: {len(leaf_cats)}")
    
    # Chạy test giới hạn số lượng danh mục nếu cần
    leaf_cats = leaf_cats[:limit]  # 👉 bỏ dòng này nếu muốn crawl toàn bộ

    os.makedirs("data/products", exist_ok=True)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
        ),
        "Accept": "application/json"
    }

    for cat in leaf_cats:
        url = f"https://tiki.vn/api/personalish/v1/blocks/listings?limit=40&category={cat['id']}&page=1"
        try:
            res = requests.get(url, headers=headers, timeout=8)
    
            if res.status_code != 200:
                print(f"⚠️ HTTP {res.status_code} for category {cat['id']}")
                continue

            try:
                res_json = res.json()
                # raw_items = res_json.get("data")
                
                if not isinstance(res_json, dict):
                    print(f"❌ res.json() không phải dict với category {cat['id']}")
                    continue

                raw_items = res_json.get("data")
                if not isinstance(raw_items, list):
                    raw_items = res_json.get("block", {}).get("data", [])
                if not isinstance(raw_items, list):
                    print(f"⚠️ Dữ liệu không phải dạng list cho category {cat['id']}")
                    continue
                simplified_items = [simplify_product_data(p) for p in raw_items]
                
                if not simplified_items: 
                    print(f"⚠️ Không có sản phẩm trong category {cat['id']}")
                    continue
                # Lưu vào file theo mã category
                with open(f"data/products/{cat['id']}.json", "w", encoding="utf-8") as f:
                    json.dump(simplified_items, f, indent=2, ensure_ascii=False)

                print(f"✅ Lưu xong category {cat['id']}: {cat['name']}")
    
            except Exception as e:
                print(f"❌ JSON decode error for category {cat['id']}: {e}")
                print(f"     Response: {res.text[:200]}")
                continue
            
        except Exception as e:
            print(f"❌ Lỗi mạng với category {cat['id']}: {e}")
        time.sleep(0.3)

    print("✅ Hoàn tất crawl sản phẩm cho các danh mục lá.")

if __name__ == "__main__":
    crawl_products_by_leaf_category()
