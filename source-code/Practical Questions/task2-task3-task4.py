import requests
import psycopg2
from datetime import datetime

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

def transform_data(notion_data):
    transformed_data = []
    for page in notion_data:
        props = page['properties']
        task_name = props['Task_name']['title'][0]["text"]["content"]
        status = props['Status']['status']['name']
        due_date = props['Due date']['date']['start']
        due_date_timestamp = datetime.strptime(due_date, "%Y-%m-%d").strftime('%Y-%m-%d %H:%M:%S')
        transformed_data.append((task_name, status, due_date_timestamp))
    return transformed_data

def insert_data_to_db(data):
    try:
        conn = psycopg2.connect(
            dbname="demoDatabase",
            user="integration-demo",
            password="password",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()
        # Create table if it doesn't exist
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS integrationDemo (
            id SERIAL PRIMARY KEY,
            task_name VARCHAR(255),
            status VARCHAR(50),
            due_date TIMESTAMP
        );
        '''
        cursor.execute(create_table_query)
        insert_query = 'INSERT INTO integrationDemo (task_name, status, due_date) VALUES (%s, %s, %s);'
        cursor.executemany(insert_query, data)

        # Commit the transaction
        conn.commit()
        print("Data inserted successfully")
        
    except Exception as e:
        print(f"Error inserting data: {e}")

    finally:
        if conn:
            cursor.close()
            conn.close()

notion_data = get_pages()

# Transform Notion data for PostgreSQL insertion
transformed_data = transform_data(notion_data)

# Insert transformed data into PostgreSQL
insert_data_to_db(transformed_data)
