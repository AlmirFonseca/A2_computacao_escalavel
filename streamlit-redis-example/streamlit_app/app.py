import streamlit as st
import redis
import time
import logging
from datetime import datetime, timedelta
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Redis connection
@st.cache_resource
def get_redis_client():
    return redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

redis_client = get_redis_client()

# Function to fetch data from Redis
@st.cache_data(ttl=10)
def fetch_data():
    purchases_per_minute = redis_client.hgetall("purchases_per_minute")
    # Deserialize purchases_per_minute
    for key in purchases_per_minute:
        purchases_per_minute[key] = json.loads(purchases_per_minute[key])
    
    ###############################################################################################################

    revenue_per_minute = redis_client.hgetall("revenue_per_minute")
    # Deserialize revenue_per_minute
    for key in revenue_per_minute:
        revenue_per_minute[key] = json.loads(revenue_per_minute[key])

    ###############################################################################################################

    most_viewed_products = redis_client.zrevrange("most_viewed_products", 0, -1, withscores=True)
    sold_out_products = redis_client.smembers("sold_out_products")

    # Get the number of stores, as keys in "purchases_per_minute" are store IDs
    num_stores = len(purchases_per_minute)
    stores = list(purchases_per_minute.keys())
    
    return stores, purchases_per_minute, revenue_per_minute, most_viewed_products, sold_out_products

def main():
    st.title("E-commerce Dashboard")

    # Interval slider for polling
    interval = st.sidebar.slider("Polling Interval (seconds)", min_value=1, max_value=60, value=10)

    # Fetch data
    store_ids, purchases_per_minute, revenue_per_minute, most_viewed_products, sold_out_products = fetch_data()

    # Sidebar for store selection
    selected_store = st.sidebar.selectbox("Select a Store", store_ids)

    st.header(f"Dashboard Insights for Store {selected_store}")
    
    # Purchases per minute
    st.subheader("Purchases per Minute")
    purchases = purchases_per_minute.get(selected_store, {"window_start": "N/A", "window_end": "N/A", "count": "N/A"})
    st.write(purchases)
    st.write(f'**{purchases.get("count")}** purchases per minute ({purchases.get("window_start")} - {purchases.get("window_end")})')

    # Revenue per minute
    st.subheader("Revenue per Minute")
    revenues = revenue_per_minute.get(selected_store, {"window_start": "N/A", "window_end": "N/A", "count": "N/A"})
    st.write(revenues)
    st.write(f'**$ {revenues.get("value"):.2f}** of revenue per minute ({revenues.get("window_start")} - {revenues.get("window_end")})')

    
    
    st.subheader("Most Viewed Products (Last Hour)")
    st.write(most_viewed_products)
    
    st.subheader("Sold Out Products")
    st.write(sold_out_products)

    # Print a message to the Streamlit app and log it
    message = "Data fetched from Redis at " + time.ctime()
    st.text(message)
    logging.info(message)

    # Polling
    time.sleep(interval)  # Polling interval
    st.rerun()

if __name__ == "__main__":
    main()
