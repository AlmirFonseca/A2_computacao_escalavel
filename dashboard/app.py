import streamlit as st
import redis
import time
import logging
from datetime import datetime, timedelta
import json
import matplotlib.pyplot as plt
import altair as alt

DEBUG = False

def remove_zero_level_nesting(d):
    """
    Remove the specific "0" level nesting from the dictionary.

    :param d: The dictionary with a specific nesting level to remove
    :return: A dictionary with the "0" level nesting removed
    """
    flattened = {}
    for outer_key, inner_dict in d.items():
        if '0' in inner_dict:
            # Promote the "0" level contents to the outer key
            flattened[outer_key] = inner_dict['0']
        else:
            # If "0" level is not present, just copy the outer_dict key-value pairs
            flattened[outer_key] = inner_dict
    return flattened

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
    
    # Purchases per minute
    ###############################################################################################################

    purchases_per_minute = redis_client.hgetall("purchases_per_minute")
    # Deserialize purchases_per_minute
    for key in purchases_per_minute:
        purchases_per_minute[key] = json.loads(purchases_per_minute[key])

    # Save the data to a file
    with open('purchases_per_minute.json', 'w') as f:
        json.dump(purchases_per_minute, f)

    # Remove the "0" level nesting from purchases_per_minute
    purchases_per_minute = remove_zero_level_nesting(purchases_per_minute)
    
    # Revenue per minute
    ###############################################################################################################

    revenue_per_minute = redis_client.hgetall("revenue_per_minute")
    # Deserialize revenue_per_minute
    for key in revenue_per_minute:
        revenue_per_minute[key] = json.loads(revenue_per_minute[key])

    # Save the data to a file
    with open('revenue_per_minute.json', 'w') as f:
        json.dump(revenue_per_minute, f)

    # Remove the "0" level nesting from revenue_per_minute
    revenue_per_minute = remove_zero_level_nesting(revenue_per_minute)

    # Unique viewers per minute
    ###############################################################################################################

    unique_users_per_minute = redis_client.hgetall("unique_users_per_minute")
    # Deserialize unique_users_per_minute
    for key in unique_users_per_minute:
        unique_users_per_minute[key] = json.loads(unique_users_per_minute[key])

    # Save the data to a file
    with open('unique_users_per_minute.json', 'w') as f:
        json.dump(unique_users_per_minute, f)

    # Most Viewed Products per hour
    ###############################################################################################################

    most_viewed_products = redis_client.hgetall("ranking_viewed_products_per_hour")
    # Deserialize unique_users_per_minute
    for key in most_viewed_products:
        most_viewed_products[key] = json.loads(most_viewed_products[key])

    # Save the data to a file
    with open('most_viewed_products.json', 'w') as f:
        json.dump(most_viewed_products, f)

    # Median of the views before buying
    ###############################################################################################################

    median_views_before_buying = redis_client.hgetall("median_views_before_buy")
    # Deserialize median_views_before_buying
    for key in median_views_before_buying:
        median_views_before_buying[key] = json.loads(median_views_before_buying[key])

    # Save the data to a file
    with open('median_views_before_buying.json', 'w') as f:
        json.dump(median_views_before_buying, f)

    # Sold out products
    ###############################################################################################################

    sold_out_products = redis_client.smembers("sold_out_products")

    # Get the number of stores, as keys in "purchases_per_minute" are store IDs
    stores = list(purchases_per_minute.keys())
    
    return stores, purchases_per_minute, revenue_per_minute, unique_users_per_minute, most_viewed_products, median_views_before_buying, sold_out_products

def main():
    st.title("E-commerce Dashboard")

    # Interval slider for polling
    interval = st.sidebar.slider("Polling Interval (seconds)", min_value=1, max_value=60, value=10)

    # Fetch data
    store_ids, purchases_per_minute, revenue_per_minute, unique_users_per_minute, most_viewed_products, median_views_before_buying, sold_out_products = fetch_data()
    # Sort store IDs
    store_ids = sorted(store_ids)

    # Shorten the store IDs to the last 10 characters
    shortened_store_ids = [store_id[-10:] for store_id in store_ids]

    # Sidebar for store selection
    selected_shortened_store_id = st.sidebar.selectbox("Select a Store", shortened_store_ids)

    # Get the full store ID based on the shortenned store ID
    selected_store_full_id = next(store_id for store_id in store_ids if store_id.endswith(selected_shortened_store_id))

    # st.write("selected_store_full_id", selected_store_full_id)
    # st.write("store_ids", store_ids)
    # st.write("shortened_store_ids", shortened_store_ids)
    # st.write("purchases_per_minute", purchases_per_minute)
    # st.write("purchases_per_minute(ALL)", purchases_per_minute.get("All"))
    # st.write("purchases_per_minute(selected)", purchases_per_minute.get(selected_store_full_id))

    # Filter the data based on the selected store
    purchases_per_minute = {selected_store_full_id: purchases_per_minute[selected_store_full_id]}
    revenue_per_minute = {selected_store_full_id: revenue_per_minute[selected_store_full_id]}
    unique_users_per_minute = {selected_store_full_id: unique_users_per_minute[selected_store_full_id]}
    most_viewed_products = {selected_store_full_id: most_viewed_products[selected_store_full_id]}

    # Update the label based on the selected store
    if selected_store_full_id == "All":
        selected_store_label = "All Stores"
    else:
        selected_store_label = f"the Store with ID {selected_shortened_store_id}"
    st.header(f"Dashboard Insights for {selected_store_label}")

    st.markdown("---")
    

    # Purchases per minute
    ###############################################################################################################

    st.subheader("Purchases per Minute")
    purchases = purchases_per_minute.get(selected_store_full_id, {"window_start": "N/A", "window_end": "N/A", "count": "N/A"})
    if DEBUG:
        st.write("purchases", purchases)
    st.write(f'**{purchases.get("count")}** purchases per minute ({purchases.get("window_start")} - {purchases.get("window_end")})')

    st.markdown("---")


    # Revenue per minute
    ###############################################################################################################
    
    st.subheader("Revenue per Minute")
    revenues = revenue_per_minute.get(selected_store_full_id, {"window_start": "N/A", "window_end": "N/A", "count": "N/A"})
    if DEBUG:
        st.write("revenues", revenues)
    st.write(f'**$ {revenues.get("amount_earned"):.2f}** of revenue per minute ({revenues.get("window_start")} - {revenues.get("window_end")})')

    st.markdown("---")


    # Unique viewers per minute
    ###############################################################################################################
    
    st.subheader("Unique Viewers per Minute")

    if DEBUG:
        st.write("unique_users_per_minute", unique_users_per_minute)

    # Flatten the dictionary
    unique_users_per_minute = list(list(unique_users_per_minute.values())[0].values())

    # Sort the data based on the unique users
    unique_users_per_minute = sorted(unique_users_per_minute, key=lambda x: x['unique_users'], reverse=True)

    # Extract names of the products and unique_users
    names = [item['name'] for item in unique_users_per_minute]
    unique_users = [item['unique_users'] for item in unique_users_per_minute]

    # Prepare data for Altair chart
    chart_data = [dict(Products=name, Unique_Users=users) for name, users in zip(names, unique_users)]

    # Create Altair chart
    chart = alt.Chart(alt.Data(values=chart_data), height=400).mark_bar().encode(
        x=alt.X('Unique_Users:Q', title='Unique Users'),
        y=alt.Y('Products:N', sort='-x', title='Products')
    ).properties(
        title='Product Viewers Ranking'
    )

    # Unique viewers per minute
    st.subheader("Unique Viewers per Minute")

    # Display chart using Streamlit
    st.altair_chart(chart, use_container_width=True)

    # Adding the timestamps as caption
    caption = f"Data window: ({unique_users_per_minute[0]['window_start']} - {unique_users_per_minute[0]['window_end']})"
    st.caption(caption)

    st.markdown("---")


    # Most Viewed Products per hour
    ###############################################################################################################
    
    st.subheader("Most Viewed Products per hour")

    if DEBUG:
        st.write("most_viewed_products", most_viewed_products)

    # Flatten the dictionary
    most_viewed_products = list(list(most_viewed_products.values())[0].values())

    # Sort the data based on the views
    most_viewed_products = sorted(most_viewed_products, key=lambda x: x['views'], reverse=True)

    # Extract names of the products and unique_users
    names = [item['name'] for item in most_viewed_products]
    views = [item['views'] for item in most_viewed_products]

    # Prepare data for Altair chart
    chart_data = [dict(Products=name, Views=views) for name, views in zip(names, views)]

    # Create Altair chart
    chart = alt.Chart(alt.Data(values=chart_data), height=400).mark_bar().encode(
        x=alt.X('Views:Q', title='Views'),
        y=alt.Y('Products:N', sort='-x', title='Products')
    ).properties(
        title='Popular Products Ranking'
    )

    # Most Viewed Products per hour
    st.subheader("Most Viewed Products per hour")

    # Display chart using Streamlit
    st.altair_chart(chart, use_container_width=True)

    # Adding the timestamps as caption
    caption = f"Data window: ({most_viewed_products[0]['window_start']} - {most_viewed_products[0]['window_end']})"
    st.caption(caption)

    st.markdown("---")


    # Median of the views before buying
    ###############################################################################################################

    st.subheader("Median of the views before buying")

    if DEBUG:
        st.write("median_views_before_buying", median_views_before_buying)

    # Flatten the dictionary
    median_views_before_buying = list(list(median_views_before_buying.values())[0].values())

    # Sort the data based on the views, in ascending order
    median_views_before_buying = sorted(median_views_before_buying, key=lambda x: x['views'])

    # Extract number of views and multiplicities
    views = [item['views'] for item in median_views_before_buying]
    view_counts = [item['count'] for item in median_views_before_buying]

    # Get the median of the view counts
    sum_view_count = sum(view_counts)
    median_index = sum_view_count // 2
    median_view = 0
    for view, view_count in zip(views, view_counts):
        median_index -= view_count
        if median_index <= 0:
            median_index = max(0, median_index)
            median_view = view
            break

    st.write(f'The median of the views before buying is **{median_view}**')
    st.caption(f'The views are from {views[0]} to {views[-1]}, with a sum of {sum_view_count} views')

    st.markdown("---")


    # # Sold out products
    # ###############################################################################################################
    
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
