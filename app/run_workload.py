"""
Generate a transactional workload against Snowflake Postgres by repeatedly
calling the `place_order` stored function from multiple worker threads.
"""

import argparse
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from .db import get_connection


def place_order_client(customer_id: int, product_id: int, quantity: int):
    """
    Call the place_order stored function once.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT place_order(%s, %s, %s);",
                (customer_id, product_id, quantity),
            )
            order_id = cur.fetchone()[0]
            conn.commit()
            return order_id
    except Exception as exc:
        conn.rollback()
        raise exc
    finally:
        conn.close()


def worker(worker_id: int, orders: int, max_customer_id: int, max_product_id: int):
    """
    Worker that submits a number of orders.
    """
    successes = 0
    failures = 0
    for _ in range(orders):
        customer_id = random.randint(1, max_customer_id)
        product_id = random.randint(1, max_product_id)
        quantity = random.randint(1, 3)
        try:
            place_order_client(customer_id, product_id, quantity)
            successes += 1
        except Exception:
            failures += 1
    return worker_id, successes, failures


def main():
    parser = argparse.ArgumentParser(
        description="Run transactional workload against Snowflake Postgres."
    )
    parser.add_argument("--workers", type=int, default=4, help="Number of worker threads")
    parser.add_argument(
        "--orders-per-worker",
        type=int,
        default=250,
        help="Number of orders each worker should submit",
    )
    parser.add_argument(
        "--max-customers",
        type=int,
        default=10,
        help="Maximum customer_id to use",
    )
    parser.add_argument(
        "--max-products",
        type=int,
        default=10,
        help="Maximum product_id to use",
    )

    args = parser.parse_args()

    total_orders = args.workers * args.orders_per_worker
    print(
        f"Starting workload: {total_orders} orders "
        f"({args.orders_per_worker} per worker, {args.workers} workers)."
    )

    start_time = time.time()
    results = []

    # Use a global random seed per process, but each worker has its own thread
    random.seed()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                worker,
                worker_id=i,
                orders=args.orders_per_worker,
                max_customer_id=args.max_customers,
                max_product_id=args.max_products,
            )
            for i in range(1, args.workers + 1)
        ]

        for future in as_completed(futures):
            results.append(future.result())

    elapsed = time.time() - start_time
    total_successes = sum(s for _, s, _ in results)
    total_failures = sum(f for _, _, f in results)

    print("Workload complete.")
    print(f"Elapsed time: {elapsed:.2f} seconds")
    print(f"Successful orders: {total_successes}")
    print(f"Failed orders: {total_failures}")
    if elapsed > 0:
        print(f"Throughput: {total_successes / elapsed:.2f} successful orders/sec")


if __name__ == "__main__":
    # Allow multiple processes to share output nicely
    threading.current_thread().name = "main"
    main()


