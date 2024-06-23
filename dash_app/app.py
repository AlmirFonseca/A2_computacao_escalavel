import dash
from dash import dcc, html
from dash.dependencies import Output, Input
import json
import redis
import threading

DEBUG = False

# Initialize the Dash app
app = dash.Dash(__name__)
server = app.server

# Redis connection
redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# Global variable to store messages
messages = {}
latest_message = {}

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
def subscribe_to_redis():
    pubsub = redis_client.pubsub()
    pubsub.subscribe(**{"ecommerce_data": handle_message})
    pubsub.run_in_thread(sleep_time=0.001)

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
    )
])

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
    
    return parsed_output

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

if __name__ == '__main__':
    # Start Redis subscription
    subscribe_to_redis()

    # Start the Dash app
    app.run_server(debug=True, host='0.0.0.0', port=5000)
