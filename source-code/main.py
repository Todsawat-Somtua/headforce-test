import requests
import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
from flask import Flask, jsonify

load_dotenv()

app = Flask(__name__)

# Constants
POSTGRESQL_CONNECTION = f"dbname='{os.getenv('DB_NAME')}' user='{os.getenv('DB_USERNAME')}' host='{os.getenv('DB_HOST')}' password='{os.getenv('DB_PASSWORD')}' port='{os.getenv('DB_PORT')}'"

# 1. API Setup and Authentication
def fetch_notion_pages():
    url = f"https://api.notion.com/v1/databases/{os.getenv('DATABASE_ID')}/query"
    headers = {
        "Authorization": f"Bearer {os.getenv('NOTION_TOKEN')}",
        "Notion-Version": f"{os.getenv('NOTION_VERSION')}",
        "Content-Type": "application/json"
    }
    response = requests.post(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

# 2. Data Fetching and Transformation
def transform_notion_data(notion_data):
    transformed_data = []
    for result in notion_data['results']:
        page_id = result['id']
        task_name = result['properties']['Task name']['title'][0]['text']['content']
        status = result['properties']['Status']['status']['name']
        due_date = result['properties']['Due date']['date']['start']
        due_date = datetime.fromisoformat(due_date).strftime('%Y-%m-%d %H:%M:%S')
        updated_at = result['properties']['Updated at']['last_edited_time']
        updated_at = datetime.fromisoformat(updated_at).astimezone().strftime('%Y-%m-%d %H:%M:%S')
        transformed_data.append((page_id, task_name, status, due_date, updated_at))
    return transformed_data

# 3. Database Setup
def create_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS notion_tasks (
        page_id VARCHAR(255) PRIMARY KEY,
        task_name VARCHAR(255) NOT NULL,
        status VARCHAR(50),
        due_date TIMESTAMP,
        updated_at TIMESTAMP
    );
    """
    conn = psycopg2.connect(POSTGRESQL_CONNECTION)
    cursor = conn.cursor()
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

# 4. Data Insertion
def insert_data_to_db(data):
    insert_query = """
    INSERT INTO notion_tasks (page_id, task_name, status, due_date, updated_at)
    VALUES (%s, %s, %s, %s, %s)
    """
    conn = psycopg2.connect(POSTGRESQL_CONNECTION)
    cursor = conn.cursor()
    cursor.executemany(insert_query, data)
    conn.commit()
    cursor.close()
    conn.close()

# 5. Bi-directional Sync (Partial Code)
def sync_data():
    notion_data = fetch_notion_pages()
    transformed_data = transform_notion_data(notion_data)
    conn = psycopg2.connect(POSTGRESQL_CONNECTION)
    cursor = conn.cursor()
    
    for task in transformed_data:
        page_id, task_name, status, due_date, updated_at = task
        cursor.execute("SELECT updated_at FROM notion_tasks WHERE page_id = %s", (page_id,))
        result = cursor.fetchone()
        
        if result:
            db_updated_at = result[0]
            if datetime.fromisoformat(updated_at) > db_updated_at:
                cursor.execute("""
                    UPDATE notion_tasks
                    SET task_name = %s, status = %s, due_date = %s, updated_at = %s
                    WHERE page_id = %s
                """, (task_name, status, due_date, updated_at, page_id))
        else:
            cursor.execute("""
                INSERT INTO notion_tasks (page_id, task_name, status, due_date, updated_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (page_id, task_name, status, due_date, updated_at))
    
    conn.commit()
    cursor.close()
    conn.close()

@app.route('/fetch_notion', methods=['GET'])
def fetch_notion():
    try:
        notion_data = fetch_notion_pages()
        transformed_data = transform_notion_data(notion_data)
        return jsonify(transformed_data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route('/create_table', methods=['POST'])
def create_db_table():
    try:
        create_table()
        return jsonify({"message": "Table created successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500 


@app.route('/insert_data', methods=['POST'])
def insert_data():
    try:
        notion_data = fetch_notion_pages()
        transformed_data = transform_notion_data(notion_data)
        insert_data_to_db(transformed_data)
        return jsonify({"message": "Data inserted successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/sync', methods=['POST'])
def sync():
    try:
        sync_data()
        return jsonify({"message": "Data synced successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)