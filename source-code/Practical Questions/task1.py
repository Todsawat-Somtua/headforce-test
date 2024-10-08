import requests
from datetime import datetime, timezone

NOTION_TOKEN = 'secret_HFnhSIdouE90GXyWoTNW4yW4c1lWxcJRN03DgvD5PUt'
DATABASE_ID = '119784cc4816805a83b3cce79d672aea'

# Define headers for API request
headers = {
    "Authorization": "Bearer " + NOTION_TOKEN,
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# Function to retrieve a list of databases from the Notion workspace
def get_pages():
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"

    payload = {"page_size": 100}
    response = requests.post(url, headers=headers, json=payload)

    data = response.json()

    import json
    with open('data.json', 'w', encoding='utf8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    results = data['results']
    return results

pages = get_pages()

for page in pages:
    page_id = page['id']
    props = page['properties']
    task_name = props['Task_name']['title'][0]["text"]["content"]
    status = props['Status']['status']['name']
    due_date = props['Due date']['date']['end']
    if due_date:
        due_date = datetime.fromisoformat(due_date)
    print(f"Task: {task_name}, Status: {status}, Due Date: {due_date}")
