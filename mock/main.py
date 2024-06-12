from graph_user_flow import *
import simulation
import argparse


def main(store_number: int, local: int):
    print("Starting simulation...")

    params = simulation.SimulationParams(
        cycle_duration=0.1,
        num_initial_users=1010,
        num_initial_products=1000,
        qtd_stock_initial=2000,
        max_simultaneus_users=1000,
        num_new_users_per_cycle=100,
        num_new_products_per_cycle=100,
        store_number=store_number,
    )

    if local:
        sim = simulation.Simulation(params)
        sim.run()
    # TODO: Implement cloud execution
    else:
        print("Cloud execution not implemented yet.")

    print("Simulation finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run simulation with specified store number.")
    parser.add_argument('--store_number', type=int, default=1, help='The store number to run the simulation for.')
    # add 1 to execute local vs 0 to execute in the cloud
    parser.add_argument('--local', type=int, default=1, help='Run the simulation locally or in the cloud.')
    args = parser.parse_args()
    main(args.store_number, args.local)




