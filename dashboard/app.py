import streamlit as st
import redis
import time
import logging
from datetime import datetime, timedelta
import json

DEBUG = False

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
    # Convert store IDs to integers
    store_ids = [int(store_id) for store_id in store_ids]
    # Sort store IDs
    store_ids = sorted(store_ids)
    # Add an option "All" to select all stores
    # store_ids.insert(0, "All")

    # Sidebar for store selection
    selected_store = st.sidebar.selectbox("Select a Store", store_ids)

    # print("selected_store", selected_store)
    # print("store_ids", store_ids)
    # print("purchases_per_minute", purchases_per_minute)

    st.write("selected_store", selected_store)
    st.write("store_ids", store_ids)
    st.write("purchases_per_minute", purchases_per_minute)

    # # If a specific store is selected, filter the data
    # if selected_store != "All":
    #     purchases_per_minute = {selected_store: purchases_per_minute[selected_store]}
    #     revenue_per_minute = {selected_store: revenue_per_minute[selected_store]}
    # else:
    #     # If all stores are selected, calculate the total purchases and revenue

    #     # Initialize total purchases and revenue as the window start/end of the first store
    #     total_purchases = {"window_start": purchases_per_minute["1"]["window_start"], "window_end": purchases_per_minute["1"]["window_end"], "count": 0}
    #     total_revenue = {"window_start": revenue_per_minute["1"]["window_start"], "window_end": revenue_per_minute["1"]["window_end"], "value": 0}

    #     # Accumulate values from stores
    #     for store in purchases_per_minute:
    #         total_purchases["count"] += purchases_per_minute[store]["count"]
    #         total_revenue["value"] += revenue_per_minute[store]["value"]

    #     # Update the data for all stores
    #     purchases_per_minute = {"All": total_purchases}
    #     revenue_per_minute = {"All": total_revenue}

    # if selected_store == "All":
    #     selected_store_label = "All Stores"
    # else:
    #     selected_store_label = f"the Store Num. {selected_store}"
    # st.header(f"Dashboard Insights for {selected_store_label}")

    # # Horizontal line
    # st.markdown("---")
    
    # # Purchases per minute
    # st.subheader("Purchases per Minute")
    # purchases = purchases_per_minute.get(selected_store, {"window_start": "N/A", "window_end": "N/A", "count": "N/A"})
    # if DEBUG:
    #     st.write(purchases)
    # st.write(f'**{purchases.get("count")}** purchases per minute ({purchases.get("window_start")} - {purchases.get("window_end")})')

    # # # Revenue per minute
    # # st.subheader("Revenue per Minute")
    # # revenues = revenue_per_minute.get(selected_store, {"window_start": "N/A", "window_end": "N/A", "count": "N/A"})
    # # if DEBUG:
    # #     st.write(revenues)
    # # st.write(f'**$ {revenues.get("value"):.2f}** of revenue per minute ({revenues.get("window_start")} - {revenues.get("window_end")})')

    
    
    # st.subheader("Most Viewed Products (Last Hour)")
    # st.write(most_viewed_products)
    
    # st.subheader("Sold Out Products")
    # st.write(sold_out_products)

    # Print a message to the Streamlit app and log it
    message = "Data fetched from Redis at " + time.ctime()
    st.text(message)
    logging.info(message)

    # Polling
    time.sleep(interval)  # Polling interval
    st.rerun()

if __name__ == "__main__":
    main()
