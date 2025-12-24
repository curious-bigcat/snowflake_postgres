"""
Continuously ingest data into Snowflake Postgres by placing new orders
at a fixed interval (default: every 20 seconds).
"""

import argparse
import random
import time

from .db import get_connection


def place_order_once(customer_id: int, product_id: int, quantity: int) -> int:
    """
    Call the place_order stored function once and return the new order_id.
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
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ingest_batch(batch_size: int, max_customers: int, max_products: int) -> tuple[int, int]:
    """
    Ingest a batch of orders. Returns (successes, failures).
    """
    successes = 0
    failures = 0
    for _ in range(batch_size):
        customer_id = random.randint(1, max_customers)
        product_id = random.randint(1, max_products)
        quantity = random.randint(1, 3)
        try:
            place_order_once(customer_id, product_id, quantity)
            successes += 1
        except Exception:
            failures += 1
    return successes, failures


def main():
    parser = argparse.ArgumentParser(
        description="Continuously ingest orders into Snowflake Postgres."
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=20,
        help="Seconds to wait between batches (default: 20)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of orders per batch (default: 10)",
    )
    parser.add_argument(
        "--max-customers",
        type=int,
        default=10,
        help="Maximum customer_id to use (default: 10)",
    )
    parser.add_argument(
        "--max-products",
        type=int,
        default=10,
        help="Maximum product_id to use (default: 10)",
    )

    args = parser.parse_args()

    print(
        f"Starting continuous ingestion: batch_size={args.batch_size}, "
        f"interval={args.interval_seconds}s"
    )

    iteration = 0
    try:
        while True:
            iteration += 1
            print(f"\nBatch {iteration}: ingesting {args.batch_size} orders...")
            successes, failures = ingest_batch(
                batch_size=args.batch_size,
                max_customers=args.max_customers,
                max_products=args.max_products,
            )
            print(
                f"Batch {iteration} complete. "
                f"successes={successes}, failures={failures}"
            )
            time.sleep(args.interval_seconds)
    except KeyboardInterrupt:
        print("\nStopping continuous ingestion.")


if __name__ == "__main__":
    main()


