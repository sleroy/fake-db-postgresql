import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import numpy as np
from functools import partial
import os
from typing import List, Dict, Set, Tuple
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text
from datetime import datetime
import random
from tqdm import tqdm
import math

# Create a Faker instance per process
fake = Faker()

def get_table_sizes(engine) -> List[tuple]:
    """Get detailed table sizes including data size, index size, and row count"""
    query = """
    SELECT 
        schemaname,
        relname as table_name,
        n_live_tup as row_count,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) as total_size,
        pg_size_pretty(pg_relation_size(schemaname||'.'||relname)) as data_size,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname) - pg_relation_size(schemaname||'.'||relname)) as index_size,
        pg_relation_size(schemaname||'.'||relname) as raw_data_size,
        pg_total_relation_size(schemaname||'.'||relname) as raw_total_size
    FROM pg_stat_user_tables
    WHERE schemaname = 'public'
    ORDER BY pg_total_relation_size(schemaname||'.'||relname) DESC;
    """
    
    with engine.connect() as conn:
        results = conn.execute(text(query))
        return results.fetchall()

def print_table_sizes(sizes, title=""):
    """Print table sizes in a formatted way"""
    print(f"\n{title}")
    print("-" * 100)
    print(f"{'Table Name':<30} {'Row Count':>12} {'Data Size':>15} {'Index Size':>15} {'Total Size':>15}")
    print("-" * 100)
    
    total_rows = 0
    total_data_size = 0
    total_index_size = 0
    total_size = 0
    
    for row in sizes:
        print(f"{row.table_name:<30} {row.row_count:>12,} {row.data_size:>15} {row.index_size:>15} {row.total_size:>15}")
        total_rows += row.row_count
        total_data_size += row.raw_data_size
        total_index_size += (row.raw_total_size - row.raw_data_size)
        total_size += row.raw_total_size
    
    print("-" * 100)
    print(f"{'TOTAL':<30} {total_rows:>12,} {sizeof_fmt(total_data_size):>15} {sizeof_fmt(total_index_size):>15} {sizeof_fmt(total_size):>15}")

def sizeof_fmt(num: int) -> str:
    """Convert bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} EB"

def init_faker():
    """Initialize Faker for each process"""
    global fake
    fake = Faker()
    # Set seed based on process ID for better randomization
    Faker.seed(os.getpid())
    random.seed(os.getpid())

def parallel_chunk_generator(func, total_size: int, chunk_size: int, num_processes: int, **kwargs) -> List[dict]:
    """Generate chunks in parallel using multiple processes"""
    chunks = []
    chunk_sizes = [chunk_size] * (total_size // chunk_size)
    if total_size % chunk_size:
        chunks.append(total_size % chunk_size)

    with ProcessPoolExecutor(max_workers=num_processes, initializer=init_faker) as executor:
        futures = [executor.submit(func, size, **kwargs) for size in chunk_sizes]
        
        for future in tqdm(futures, total=len(futures), desc=f"Generating {func.__name__}"):
            chunk = future.result()
            chunks.extend(chunk)
    
    return chunks

def parallel_insert_chunks(engine, table_name: str, chunks: List[dict], insert_chunk_size: int, 
                         num_threads: int):
    """Insert chunks in parallel using multiple threads"""
    df = pd.DataFrame(chunks)
    total_rows = len(df)
    splits = np.array_split(df, math.ceil(total_rows / insert_chunk_size))
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for chunk in splits:
            futures.append(
                executor.submit(
                    lambda c: c.to_sql(
                        table_name,
                        engine,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=insert_chunk_size
                    ),
                    chunk
                )
            )
        
        for future in tqdm(futures, total=len(futures), desc=f"Inserting {table_name}"):
            future.result()

def generate_inventory_chunk(chunk_size: int, film_ids: List[int], store_ids: List[int]) -> List[dict]:
    """Generate inventory records in parallel"""
    current_time = datetime.now()
    
    return [{
        'film_id': random.choice(film_ids),
        'store_id': random.choice(store_ids),
        'last_update': current_time
    } for _ in range(chunk_size)]

def generate_rental_chunk(chunk_size: int, inventory_ids: List[int], 
                         customer_ids: List[int], staff_ids: List[int]) -> List[dict]:
    """Generate rental records in parallel"""
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2022, 7, 31)
    
    return [{
        'rental_date': fake.date_time_between(start_date=start_date, end_date=end_date),
        'inventory_id': random.choice(inventory_ids),
        'customer_id': random.choice(customer_ids),
        'return_date': fake.date_time_between(start_date=start_date, end_date=end_date) if random.random() > 0.2 else None,
        'staff_id': random.choice(staff_ids),
        'last_update': datetime.now()
    } for _ in range(chunk_size)]

def generate_payment_chunk(chunk_size: int, rental_ids: List[int], 
                         customer_ids: List[int], staff_ids: List[int]) -> List[dict]:
    """Generate payment records in parallel"""
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2022, 7, 31)
    
    return [{
        'customer_id': random.choice(customer_ids),
        'staff_id': random.choice(staff_ids),
        'rental_id': random.choice(rental_ids),
        'amount': round(random.uniform(0.99, 9.99), 2),
        'payment_date': fake.date_time_between(start_date=start_date, end_date=end_date)
    } for _ in range(chunk_size)]

def parallel_bulk_insert_additional_data(num_records: int, host: str, user: str, 
                                       password: str, database: str, num_processes: int = None,
                                       num_threads: int = None):
    """Generate and insert additional records using parallel processing"""
    
    # Determine optimal number of processes and threads
    if num_processes is None:
        num_processes = mp.cpu_count()
    if num_threads is None:
        num_threads = min(32, num_processes * 2)  # Reasonable thread pool size
        
    print(f"Using {num_processes} processes for data generation")
    print(f"Using {num_threads} threads for database insertion")
    
    # Create database engine with larger pool size for parallel operations
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:5432/{database}',
        pool_size=num_threads,
        max_overflow=num_threads * 2,
        pool_timeout=30
    )
    
    # Get initial table sizes
    initial_sizes = get_table_sizes(engine)
    print_table_sizes(initial_sizes, "Initial Table Sizes")
    
    # Calculate chunk sizes
    generation_chunk_size = max(1000, num_records // (num_processes * 4))
    insert_chunk_size = max(100, generation_chunk_size // 10)
    
    print(f"Generation chunk size: {generation_chunk_size:,}")
    print(f"Insert chunk size: {insert_chunk_size:,}")
    
    # Get necessary IDs
    with engine.connect() as conn:
        film_ids = [row[0] for row in conn.execute(text("SELECT film_id FROM film"))]
        store_ids = [row[0] for row in conn.execute(text("SELECT store_id FROM store"))]
        customer_ids = [row[0] for row in conn.execute(text("SELECT customer_id FROM customer"))]
        staff_ids = [row[0] for row in conn.execute(text("SELECT staff_id FROM staff"))]
        
    # Generate and insert inventory
    print("\nGenerating inventory records...")
    inventory_chunks = parallel_chunk_generator(
        generate_inventory_chunk,
        num_records,
        generation_chunk_size,
        num_processes,
        film_ids=film_ids,
        store_ids=store_ids
    )
    
    print("Inserting inventory records...")
    parallel_insert_chunks(engine, 'inventory', inventory_chunks, insert_chunk_size, num_threads)
    
    # Get inventory IDs for rentals
    with engine.connect() as conn:
        inventory_ids = [row[0] for row in conn.execute(text("SELECT inventory_id FROM inventory"))]
    
    # Generate and insert rentals
    print("\nGenerating rental records...")
    rental_chunks = parallel_chunk_generator(
        generate_rental_chunk,
        num_records,
        generation_chunk_size,
        num_processes,
        inventory_ids=inventory_ids,
        customer_ids=customer_ids,
        staff_ids=staff_ids
    )
    
    print("Inserting rental records...")
    parallel_insert_chunks(engine, 'rental', rental_chunks, insert_chunk_size, num_threads)
    
    # Get rental IDs for payments
    with engine.connect() as conn:
        rental_ids = [row[0] for row in conn.execute(text("SELECT rental_id FROM rental"))]
    
    # Generate and insert payments
    print("\nGenerating payment records...")
    payment_chunks = parallel_chunk_generator(
        generate_payment_chunk,
        num_records,
        generation_chunk_size,
        num_processes,
        rental_ids=rental_ids,
        customer_ids=customer_ids,
        staff_ids=staff_ids
    )
    
    print("Inserting payment records...")
    parallel_insert_chunks(engine, 'payment', payment_chunks, insert_chunk_size, num_threads)
    
    # Reset sequences
    print("\nResetting sequences...")
    with engine.begin() as conn:
        conn.execute(text("""
            SELECT setval(pg_get_serial_sequence('inventory', 'inventory_id'), (SELECT MAX(inventory_id) FROM inventory));
            SELECT setval(pg_get_serial_sequence('rental', 'rental_id'), (SELECT MAX(rental_id) FROM rental));
            SELECT setval(pg_get_serial_sequence('payment', 'payment_id'), (SELECT MAX(payment_id) FROM payment));
        """))
    
 # Get final table sizes
    final_sizes = get_table_sizes(engine)
    print_table_sizes(final_sizes, "Final Table Sizes")
    
    # Print size differences
    print("\nSize Changes:")
    print("-" * 100)
    print(f"{'Table Name':<30} {'Row Difference':>15} {'Size Difference':>20}")
    print("-" * 100)
    
    initial_sizes_dict = {row.table_name: row for row in initial_sizes}
    final_sizes_dict = {row.table_name: row for row in final_sizes}
    
    for table_name in final_sizes_dict:
        if table_name in initial_sizes_dict:
            initial = initial_sizes_dict[table_name]
            final = final_sizes_dict[table_name]
            row_diff = final.row_count - initial.row_count
            size_diff = final.raw_total_size - initial.raw_total_size
            if row_diff > 0 or size_diff > 0:
                print(f"{table_name:<30} {row_diff:>15,} {sizeof_fmt(size_diff):>20}")
    
    print("\nAll operations completed successfully!")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate additional records for largest tables in Pagila database')
    parser.add_argument('num_records', type=int, help='Number of records to generate')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--user', default='postgres', help='PostgreSQL username')
    parser.add_argument('--password', default='postgres', help='PostgreSQL password')
    parser.add_argument('--database', default='pagila', help='PostgreSQL database name')
    parser.add_argument('--processes', type=int, help='Number of processes to use (default: CPU count)')
    parser.add_argument('--threads', type=int, help='Number of threads to use (default: processes * 2)')
    args = parser.parse_args()
    
    parallel_bulk_insert_additional_data(
        args.num_records,
        args.host,
        args.user,
        args.password,
        args.database,
        args.processes,
        args.threads
    )
