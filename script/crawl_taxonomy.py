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
    def crawl_recursive(parent_id, level=2):
        url = f"https://tiki.vn/api/v2/categories?include=children&parent_id={parent_id}"
        try: 
            res = requests.get(url, timeout=5).json()
            children = res.get("data",[])
        except Exception as e:
            print(f"Error fetching data for category {parent_id}: {e}")
            return
        for item in children:
            node = {
                "id": item["id"],
                "name": item["name"],
                "parent_id": item["parent_id"],
                "level": level,
                "is_leaf": item["is_leaf"],                
            } 
            result.append(node)
            
            if not item.get("is_leaf", False):
                crawl_recursive(item["id"], level + 1)
            time.sleep(0.3)
    print("Lay danh sach root category tu trang cho html")
    root_ids = get_root_categories_ids()
    
    #tu them node root
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

    print(f"Hoan tat crawl. Da luu {len(result)} danh muc vao data/taxomnomy")        
if __name__ == "__main__":
    crawl_all_taxonomy()