import requests, json, time, os
from bs4 import BeautifulSoup
import  re

def get_root_categories_ids():
    url = "https://tiki.vn/"
    headers = {"User-Agent": "Mozilla/5.0"}
    res= requests.get(url, headers=headers)
    soup = BeautifulSoup(res.content, "html.parser")
    
    ids = set()
    for a in soup.find_all("a", href=True):
        match = re.search(r"/c(\d+)", a["href"])
        if match:
            ids.add(int(match.group(1)))
   
    return sorted(list(ids))
# result = get_root_categories_ids()
# print(result)
# print(len(result))

def crawl_all_taxonomy():
    result = []
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0"
    })

    def crawl_recursive(parent_id, level=2):
        url = f"https://tiki.vn/api/v2/categories?include=children&parent_id={parent_id}"
        try:
            res = session.get(url, timeout=8)
            if res.status_code != 200:
                print(f"⚠️ Status {res.status_code} for parent_id {parent_id}")
                return
            try:
                json_data = res.json()
            except Exception as e:
                print(f"❌ JSON error for category {parent_id}: {e}")
                print(f"    ↪ Content: {res.text[:200]}")
                return

            children = json_data.get("data", [])
        except Exception as e:
            print(f"❌ Error fetching data for category {parent_id}: {e}")
            return

        for item in children:
            node = {
                "id": item["id"],
                "name": item["name"],
                "parent_id": item.get("parent_id"),
                "level": level,
                "is_leaf": item.get("is_leaf", False),
                "url_path": item.get("url_path")
            }
            result.append(node)

            if not item.get("is_leaf", False):
                crawl_recursive(item["id"], level + 1)

            time.sleep(0.3)

    print("🚀 Lấy danh sách root category từ HTML trang chủ...")
    root_ids = get_root_categories_ids()

    for root_id in root_ids:
        node = {
            "id": root_id,
            "name": f"ROOT_{root_id}",
            "parent_id": None,
            "level": 1,
            "url_path": None,
            "is_leaf": False
        }
        result.append(node)
        crawl_recursive(root_id, level=2)

    os.makedirs("data", exist_ok=True)
    with open("data/taxonomy.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"✅ Hoàn tất crawl. Đã lưu {len(result)} danh mục vào data/taxonomy.json")        
if __name__ == "__main__":
    crawl_all_taxonomy()