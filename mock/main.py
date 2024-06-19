from graph_user_flow import *
import simulation
import argparse
import multiprocessing
import os
import uuid

def simulate_store(store_id_unique, logs_folder, requests_folder):
    print(f"Starting simulation for Store {store_id_unique}...")
    
    params = simulation.SimulationParams(
        cycle_duration=0.1,
        num_initial_users=1500,
        num_initial_products=1000,
        qtd_stock_initial=2000,
        max_simultaneus_users=2000,
        num_new_users_per_cycle=100,
        num_new_products_per_cycle=100,
        store_id=store_id_unique,
        logs_folder=logs_folder,
        requests_folder=requests_folder
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
    os.mkdir(f"{folder_name}/requests")
    print("Subfolders logs and requests created successfully.")
    return f"{folder_name}/logs", f"{folder_name}/requests"

def main(num_stores: int, local: int):

    # Create the main folder for this machine
    logs_folder, requests_folder = create_mock_files_folder("mock_files")

    if local:
        # Run simulations locally using multiprocessing
        pool = multiprocessing.Pool(processes=num_stores)
        logs_folder = [logs_folder] * num_stores
        print(logs_folder)
        requests_folder = [requests_folder] * num_stores
        print(requests_folder)
        stores_id = [uuid.uuid4() for _ in range(num_stores)]
        stores_id = [str(i) + "_" + str(store_id) for i, store_id in enumerate(stores_id)]

        pool.starmap(simulate_store, zip(stores_id, logs_folder, requests_folder))
        pool.close()
        pool.join()
    else:
        print("Cloud execution not implemented yet.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the simulation")
    parser.add_argument("--num_stores", type=int, help="Number of stores to simulate", default=10)
    parser.add_argument("--local", type=int, help="Run locally or in the cloud (1 for local, 0 for cloud)", default=1)
    args = parser.parse_args()

    main(args.num_stores, args.local)
