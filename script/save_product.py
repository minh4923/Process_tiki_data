import os
import json
import requests
import time

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
                data = res.json()
            except Exception as e:
                print(f"❌ JSON decode error for category {cat['id']}: {e}")
                print(f"    ↪ Response: {res.text[:200]}")
                continue

            # Lưu vào file theo mã category
            with open(f"data/products/{cat['id']}.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            print(f"✅ Lưu xong category {cat['id']}: {cat['name']}")

        except Exception as e:
            print(f"❌ Lỗi mạng với category {cat['id']}: {e}")
        time.sleep(0.3)

    print("✅ Hoàn tất crawl sản phẩm cho các danh mục lá.")

if __name__ == "__main__":
    crawl_products_by_leaf_category()
