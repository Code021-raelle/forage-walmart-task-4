import pandas as pd
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('shipment_database.db')
cursor = conn.cursor()


# Function to insert data into the table
def insert_data(table, data):
    placeholders = ', '.join('?' * len(data.columns))
    columns = ', '.join(data.columns)
    sql = f'INSERT INTO {table} ({columns}) VALUES ({placeholders})'
    cursor.executemany(sql, data.values.tolist())
    conn.commit()

# Load and insert data from shipping_data_0.csv
data_0 = pd.read_csv('data/shipping_data_0.csv')
insert_data('table_0', data_0)

# Load data from shippint_data_1.csv and shippint_data_2.csv
data_1 = pd.read_csv('data/shipping_data_1.csv')
data_2 = pd.read_csv('data/shipping_data_2.csv')

# Merge data_1 and data_2 on shipping_identifier
merged_data = pd.merge(data_1, data_2, on='shipping_identifier')

grouped_data = merged_data.groupby(['shipping_identifier', 'product_name', 'origin', 'destination']).agg({
    'quantity': 'sum'
}).reset_index()

# Insert the merged and transformed data into the database
insert_data('shipment_database', grouped_data)

# Close the database connection
conn.close()
