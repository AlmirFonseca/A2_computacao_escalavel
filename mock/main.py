from graph_user_flow import *
from confluent_kafka import Consumer
import simulation
import argparse
import multiprocessing
import os
import uuid

def simulate_store(store_id_unique, logs_folder):
    print(f"Starting simulation for Store {store_id_unique}...")
    
    params = simulation.SimulationParams(
        cycle_duration=5,
        num_initial_users=100,
        num_initial_products=100,
        qtd_stock_initial=100,
        max_simultaneus_users=15,
        num_new_users_per_cycle=3,
        num_new_products_per_cycle=3,
        store_id=store_id_unique,
        logs_folder=logs_folder,
    )

    sim = simulation.Simulation(params)
    sim.run()

    print(f"Simulation for Store {store_id_unique} finished.")

# create the main folder for this machine, deleting all its contents if it already exists
def create_mock_files_folder(folder_name):
    if os.path.exists(folder_name):
        os.system(f"rm -rf {folder_name}")
    os.mkdir(folder_name)
    print(f"Folder {folder_name} created successfully.")
    # create the subfolders logs and requests
    os.mkdir(f"{folder_name}/logs")
    print("Subfolders logs created successfully.")
    return f"{folder_name}/logs"

def main(num_stores: int, local: int):

    # Create the main folder for this machine
    logs_folder = create_mock_files_folder("mock_files")

    if local:
        # Run simulations locally using multiprocessing
        pool = multiprocessing.Pool(processes=num_stores)
        logs_folder = [logs_folder] * num_stores
        print(logs_folder)
        stores_id = [uuid.uuid4() for _ in range(num_stores)]
        stores_id = [str(i) + "_" + str(store_id) for i, store_id in enumerate(stores_id)]

        pool.starmap(simulate_store, zip(stores_id, logs_folder))
        pool.close()
        pool.join()
    else:
        print("Cloud execution not implemented yet.")


# Function to consume messages from a Kafka topic
def consume_messages(broker_url, topic_name):
    print(f'Consuming messages from topic: {topic_name}')
    try:
        try:
            subscriber = Consumer({'bootstrap.servers': broker_url, 'group.id': 'event_subscriber'})
        except Exception as e:
            print(f'An error occurred connecting to Kafka: {e}')
            return
        
        subscriber.subscribe([topic_name])

        while True:
            message = subscriber.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                print(f'Error consuming message: {message.error()}')
            else:
                message = message.value().decode()
                print(f'\n========= USER {message["user_id"]} OF STORE {message["store_id"]} GOT A COUPON =========\n')
    except Exception as e:
        print(f'An error occurred consuming messages: {e}')


if __name__ == "__main__":
    KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
    INPUT_TOPIC = os.environ.get('INPUT_TOPIC')

    # Add a consumer to the Kafka topic
    multiprocessing.Process(target=consume_messages, args=(KAFKA_BROKER, INPUT_TOPIC)).start()

    parser = argparse.ArgumentParser(description="Run the simulation")
    parser.add_argument("--num_stores", type=int, help="Number of stores to simulate", default=5)
    parser.add_argument("--local", type=int, help="Run locally or in the cloud (1 for local, 0 for cloud)", default=1)
    args = parser.parse_args()

    main(args.num_stores, args.local)
