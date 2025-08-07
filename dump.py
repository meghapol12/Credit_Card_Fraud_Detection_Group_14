import os
import pandas as pd
import mysql.connector
from mysql.connector import Error

# === STEP 1: Load CSV ===
print("📄 Reading CSV...")
try:
    csv_file = "balanced_train_data.csv"
    df = pd.read_csv(csv_file)
    print("✅ CSV loaded successfully!")
except FileNotFoundError:
    print("❌ Error: CSV file not found. Check the path.")
    exit()

# === STEP 2: Connect to MySQL ===
try:
    print("🔌 Connecting to MySQL...")
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="frauddb"
    )
    cursor = connection.cursor()
    print("🔌 Connected to MySQL database 'frauddb'")

    # === STEP 3: Create table if not exists ===
    table_name = "fraud_data"
    create_query = """
    CREATE TABLE IF NOT EXISTS fraud_data (
        amt DOUBLE,
        age INT,
        distance DOUBLE,
        amt_per_capita DOUBLE,
        is_far INT,
        hour_bucket_encoded DOUBLE,
        category_grocery_pos DOUBLE,
        category_home DOUBLE,
        category_shopping_pos DOUBLE,
        category_kids_pets DOUBLE,
        category_shopping_net DOUBLE,
        category_entertainment DOUBLE,
        category_food_dining DOUBLE,
        category_personal_care DOUBLE,
        category_health_fitness DOUBLE,
        category_misc_pos DOUBLE,
        category_misc_net DOUBLE,
        category_grocery_net DOUBLE,
        category_travel DOUBLE,
        gender_M DOUBLE,
        city_freq INT,
        is_fraud INT
    )
    """
    cursor.execute(create_query)
    print(f"✅ Table '{table_name}' is ready")

    # === STEP 4: Insert data in chunks ===
    insert_query = f"""
        INSERT INTO {table_name} VALUES (
            {', '.join(['%s'] * len(df.columns))}
        )
    """
    rows = [tuple(row) for row in df.values]

    print(f"🚚 Inserting {len(rows)} rows in chunks...")

    chunk_size = 10000
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        cursor.executemany(insert_query, chunk)
        connection.commit()
        print(f"✅ Inserted rows {i} to {i + len(chunk) - 1}")

    print("🎉 All data inserted successfully into MySQL!")

except Error as e:
    print(f"❌ MySQL Error: {e}")

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("🔌 MySQL connection closed.")
