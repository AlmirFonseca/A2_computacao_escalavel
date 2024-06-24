import dash
import json
import redis
import threading
import time

from datetime import datetime
from dash import dcc, html
from dash.dependencies import Output, Input

DEBUG = False

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=['/assets/styles.css'])
server = app.server

# Redis connection
redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# Global variable to store messages
messages = {}
latest_message = {}
latest_result = None
formatted_price_monitor_result = ""

# Function to handle incoming messages
def handle_message(message):
    global latest_message
    try:
        message_data = json.loads(message['data'])
        latest_message = message_data
        print("Received message:", json.dumps(message_data, indent=2))

        task_name = message_data.pop('task_name', 'N/A')
        store_ids = list(message_data.keys())
        print(store_ids)
        
        for store_id in store_ids:
            if store_id not in messages:
                messages[store_id] = {}
            messages[store_id][task_name] = message_data[store_id]

    except (TypeError, json.JSONDecodeError) as e:
        print(f"Error decoding message: {e}")

# Function to subscribe to Redis channels
def subscribe_to_insights():
    pubsub = redis_client.pubsub()
    pubsub.subscribe(**{"ecommerce_data": handle_message})
    pubsub.run_in_thread(sleep_time=0.001)
    
# Subscribe to Redis channel for price monitor results
def subscribe_to_price_monitor_results():
    pubsub = redis_client.pubsub()
    pubsub.subscribe(**{'price_monitor_job_results': handle_price_monitor_result_message})
    thread = pubsub.run_in_thread(sleep_time=0.001)
    return thread

# Function to parse the price monitor result
def parse_price_monitor_result(result):
    if not result:
        return "No result available."
    
    time_window = result.get('time_window', 'N/A')
    discount_percentage = result.get('discount_percentage', 'N/A')
    timestamp = result.get('timestamp', 'N/A')
    formatted_timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")
    
    # Initialize the formatted result
    parsed_result = f"Showing offers in the period of {time_window} months with {discount_percentage}% of discount (at {formatted_timestamp}):\n"

    # If there are no deals, add a message and return
    if not result.get('deals', []):
        return parsed_result + "No deals found.\n"

    # Evaluate the discount percentage for each product
    for deal in result.get('deals', []):
        product_name = deal.get('name', 'N/A')
        product_id = deal.get('id', 'N/A')
        store_id = deal.get('store_id', 'N/A')
        price = float(deal.get('price', 0))
        average_price = float(deal.get('average_price', 0))
        deal['evaluated_discount_percentage'] = ((average_price - price) / average_price) * 100

    # Order by the discount percentage (descending order)
    result['deals'] = sorted(result['deals'], key=lambda x: x['evaluated_discount_percentage'], reverse=True)

    # Iterate over the ordered deals and format the result
    for deal in result.get('deals', []):
        product_name = deal.get('name', 'N/A')
        product_id = deal.get('id', 'N/A')
        store_id = deal.get('store_id', 'N/A')
        price = float(deal.get('price', 0))
        average_price = float(deal.get('average_price', 0))
        evaluated_discount_percentage = deal.get('evaluated_discount_percentage', 0)
        
        parsed_result += f" - Product \"{product_name}\" (ID {product_id}): from ${average_price:.2f} to ${price:.2f} ({evaluated_discount_percentage:.2f}% of discount) in store {store_id}\n"
    
    return parsed_result

# Handle incoming results
def handle_price_monitor_result_message(message):
    global latest_result, formatted_price_monitor_result
    data = message['data']
    if data:
        try:
            latest_result = json.loads(data)
            print("Received result:", latest_result)

            # Parse and store the formatted result
            formatted_price_monitor_result = parse_price_monitor_result(latest_result)

        except json.JSONDecodeError:
            print("Error decoding the result message.")
            formatted_result = "Error decoding the result message."

def load_store_ids():
    return sorted(messages.keys())

# Layout of the app
app.layout = html.Div([
    html.H1("E-commerce Dashboard"),
    html.H2("Select Store"),
    dcc.Dropdown(
        id='store-dropdown',
        options=[],  # Initially no options
        value=None,
        clearable=False
    ),
    html.Div(id='output', style={'whiteSpace': 'pre-wrap', 'wordBreak': 'break-all'}),
    dcc.Interval(
        id='interval-component',
        interval=1*1000,  # in milliseconds
        n_intervals=0
    ),
    html.H2("Price Monitor"),
    html.H4("Select the number of months:"),
    dcc.Slider(
        id='months-slider',
        min=1,
        max=48,
        step=1,
        marks=None,
        value=12,
        tooltip={'placement': 'bottom', 'always_visible': True, 'template': '{value} months'}
    ),
    html.H4("Select the desired discount percentage:"),
    dcc.Slider(
        id='discount-slider',
        min=0,
        max=100,
        step=0.1,
        marks=None,
        value=10,
        tooltip={'placement': 'bottom', 'always_visible': True, 'template': '{value}% discount'}),
    html.Button('Ask for offers', id='submit-button', n_clicks=0),
    html.Div(id='container-button-basic'),
    dcc.Interval(
        id='update-interval',
        interval=500,  # Update every 0.5 second
        n_intervals=0
    ),
    html.Div(id='result-output')
])

@app.callback(
    Output('result-output', 'children'),
    [Input('update-interval', 'n_intervals')]
)
def update_result_output(n):
    return formatted_price_monitor_result

# Callback to update the dropdown options dynamically
@app.callback(
    Output('store-dropdown', 'options'),
    [Input('interval-component', 'n_intervals')]
)
def update_dropdown_options(n):
    options = [{'label': store_id, 'value': store_id} for store_id in load_store_ids()]
    return options

# Callback to update the output with the selected store
@app.callback(
    Output('output', 'children'),
    [Input('interval-component', 'n_intervals'),
     Input('store-dropdown', 'value')]
)
def update_output(n, selected_store):
    if not selected_store:
        return 'Please select a store.'
    
    filtered_data = messages.get(selected_store, {})
    parsed_output = ""
    
    for task, data in filtered_data.items():
        if DEBUG:
            parsed_output += f"\n\n{task}:\n"
            parsed_output += json.dumps(data, indent=2)
            parsed_output += f"\nParsed Data for {task}:\n"
            
        if task == 'purchases_per_minute':
            parsed_output += parse_purchases(data)
        elif task == 'revenue_per_minute':
            parsed_output += parse_revenue(data)
        elif task == 'unique_users_per_minute':
            parsed_output += parse_unique_users(data)
        elif task == 'ranking_viewed_products_per_hour':
            parsed_output += parse_most_viewed_products(data)
        elif task == 'median_views_before_buy':
            parsed_output += parse_median_views_before_buying(data)
        elif task == 'without_stock':
            parsed_output += parse_without_stock(data)
    
    return parsed_output

@app.callback(
    [Output('container-button-basic', 'children'),
     Output('submit-button', 'n_clicks')],
    [Input('submit-button', 'n_clicks')],
    [Input('months-slider', 'value'),
     Input('discount-slider', 'value')]
)
def update_output(n_clicks, months, discount):
    global formatted_price_monitor_result
    if n_clicks > 0 and discount > 0:
        message = {
            'task_name': 'price_monitor_job',
            'time_window': months,
            'discount_percentage': discount
        }
        redis_client.publish('price_monitor_channel', json.dumps(message))
        print(f"Sent PRICE_MONITOR message: {json.dumps(message, indent=2)}")
        formatted_price_monitor_result = "Waiting for the result...\n"

        return f'Offer request sent asking for products with a discount of at least {discount}% in the past {months} months (at {time.ctime()}).\n', 0
    return 'Set discount greater than 0% to enable offer request and click the button to request offers.\n', n_clicks

# Parsing functions
def parse_purchases(data):
    if not data:
        return "No data available.\n"
    
    data = data.get('0', {})
    return f"\n\nPurchases per minute (from {data.get('window_start', 'N/A')} to {data.get('window_end', 'N/A')}): {data.get('count', 'N/A')}\n"

def parse_revenue(data):
    if not data:
        return "No data available.\n"
    
    data = data.get('0', {})
    return f"\n\nRevenue per minute (from {data.get('window_start', 'N/A')} to {data.get('window_end', 'N/A')}): ${data.get('amount_earned', 'N/A'):.2f}\n"

def parse_unique_users(data):
    if not data:
        return "No data available.\n"
    
    result = f"\n\nUnique Users (from {data.get('0').get('window_start', 'N/A')} to {data.get('0').get('window_end', 'N/A')}):\n"
    for rank, product in enumerate(data.values()):
        result += f"#{rank+1}: Product {product.get('name', 'N/A')}: {product.get('unique_users', 'N/A')} unique users\n"

    return result

def parse_most_viewed_products(data):
    if not data:
        return "No data available.\n"
    
    result = f"\n\nMost Viewed Products (from {data.get('0').get('window_start', 'N/A')} to {data.get('0').get('window_end', 'N/A')}):\n"
    for rank, product in enumerate(data.values()):
        result += f"#{rank+1}: Product {product.get('name', 'N/A')}: {product.get('views', 'N/A')} views\n"

    return result

def parse_median_views_before_buying(data):
    if not data:
        return "No data available.\n"

    # Flatten the dictionary
    view_count_tuples = []
    for data_dict in data.values():
        view_count_tuples.append((data_dict.get('views', 0), data_dict.get('count', 0)))

    # Sort the data based on the views, in ascending order
    view_count_tuples = sorted(view_count_tuples, key=lambda x: x[0])

    # Extract number of views and multiplicities
    views = [item[0] for item in view_count_tuples]
    view_counts = [item[1] for item in view_count_tuples]

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

    return f"\n\nThe median of the views before buying is {median_view} (from {views[0]} to {views[-1]}, with a sum of {sum_view_count} views)\n"

def parse_without_stock(data):
    if not data:
        return "No data available.\n"

    return f"\n\nProducts Sold Without Stock: {data.get('0').get('products_sold_without_stock', '0')} units\n"

if __name__ == '__main__':
    # Start Redis subscription
    subscribe_to_insights()
    subscribe_to_price_monitor_results()

    # Start the Dash app
    app.run_server(debug=True, host='0.0.0.0', port=5000)
