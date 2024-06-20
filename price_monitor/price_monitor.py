import os
import psycopg2
from datetime import datetime, timedelta

# Database connection settings
conn_params = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT')
}

def get_price_deals(months, discount_percent):
    # Establish connection to the database
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    
    # Calculate the date range based on the user's input
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30 * months)
    
    # Calculate the average prices of products within the specified period
    cursor.execute("""
        SELECT product_id, AVG(price) as average_price 
        FROM conta_verde.price_history
        WHERE recorded_at BETWEEN %s AND %s
        GROUP BY product_id
    """, (start_date, end_date))
    
    avg_prices = cursor.fetchall()
    
    deals = []
    for product_id, avg_price in avg_prices:
        # Determine the threshold price below which a product is considered a deal
        threshold_price = avg_price * (1 - discount_percent / 100)
        
        # Find products with current prices below the threshold
        cursor.execute("""
            SELECT p.id, p.name, p.price 
            FROM conta_verde.products p
            WHERE p.id = %s AND p.price < %s
        """, (product_id, threshold_price))
        
        product = cursor.fetchone()
        if product:
            deals.append(product)
    
    # Close the cursor and the database connection
    cursor.close()
    conn.close()
    
    return deals

if __name__ == "__main__":
    # Prompt the user to enter the parameters
    months = int(input("Enter the number of months to consider: "))
    discount_percent = float(input("Enter the desired discount percentage: "))
    
    # Find products with significant discounts
    deals = get_price_deals(months, discount_percent)
    
    if deals:
        print(f"Products with prices significantly below average over the last {months} months:")
        for deal in deals:
            print(f"ID: {deal[0]}, Name: {deal[1]}, Current Price: R${deal[2]:.2f}")
    else:
        print("No products found matching the specified criteria.")
