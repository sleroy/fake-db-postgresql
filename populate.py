import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import numpy as np
from functools import partial
import os
from typing import List, Dict, Set, Tuple
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import random
from tqdm import tqdm
import math
from psycopg2.extras import execute_values
from io import StringIO
import csv

# Create a Faker instance per process
fake = Faker()

def init_faker():
    """Initialize Faker and numpy random seed for each process"""
    global fake
    fake = Faker()
    # Set seed based on process ID for better randomization
    process_seed = os.getpid()
    Faker.seed(process_seed)
    np.random.seed(process_seed)

def disable_indexes(engine, table_name):
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {table_name} DISABLE TRIGGER ALL;"))

def enable_indexes(engine, table_name):
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {table_name} ENABLE TRIGGER ALL;"))

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

def generate_inventory_chunk(chunk_size: int, film_ids: List[int], store_ids: List[int]) -> List[dict]:
    """Generate inventory records (no dates needed)"""
    current_time = datetime.now()
    film_id = film_ids[0]
    store_id = store_ids[0]
    
    return [{
        'film_id': film_id,
        'store_id': store_id,
        'last_update': current_time
    } for _ in range(chunk_size)]

def generate_rental_chunk(chunk_size: int, inventory_ids: List[int], 
                         customer_ids: List[int], staff_ids: List[int],
                         start_date: datetime) -> List[dict]:
    """Generate rental records with sequential timestamps"""
    current_time = datetime.now()
    staff_id = staff_ids[0]
    
    # Create records with sequential timestamps
    rentals = []
    inventory_index = 0
    customer_index = 0
    
    # Use smaller time increments to avoid overflow
    minutes_increment = 1
    
    for i in range(chunk_size):
        try:
            # Generate sequential rental dates with smaller increments
            rental_date = start_date + timedelta(minutes=i * minutes_increment)
            return_date = rental_date + timedelta(days=3)
            
            # Cycle through inventory_ids and customer_ids
            inventory_id = inventory_ids[inventory_index]
            customer_id = customer_ids[customer_index]
            
            rentals.append({
                'rental_date': rental_date,
                'inventory_id': inventory_id,
                'customer_id': customer_id,
                'return_date': return_date,
                'staff_id': staff_id,
                'last_update': current_time
            })
            
            # Increment indices
            inventory_index = (inventory_index + 1) % len(inventory_ids)
            if inventory_index == 0:
                customer_index = (customer_index + 1) % len(customer_ids)
                
        except OverflowError:
            # If we hit an overflow, reset the date to start_date
            rental_date = start_date
            return_date = rental_date + timedelta(days=3)
            
            # Continue with the same logic
            inventory_id = inventory_ids[inventory_index]
            customer_id = customer_ids[customer_index]
            
            rentals.append({
                'rental_date': rental_date,
                'inventory_id': inventory_id,
                'customer_id': customer_id,
                'return_date': return_date,
                'staff_id': staff_id,
                'last_update': current_time
            })
            
            # Increment indices
            inventory_index = (inventory_index + 1) % len(inventory_ids)
            if inventory_index == 0:
                customer_index = (customer_index + 1) % len(customer_ids)
    
    return rentals

def generate_payment_chunk(chunk_size: int, rental_ids: List[int], 
                         customer_ids: List[int], staff_ids: List[int]) -> List[dict]:
    """Generate payment records with random dates within 2022-01 to 2022-07"""
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2022, 7, 31)  # Last day of July 2022
    delta = end_date - start_date
    days = delta.days
    
    # Generate random dates within the partition range
    random_days = np.random.randint(0, days, size=chunk_size)
    payment_dates = [start_date + timedelta(days=int(x)) for x in random_days]
    
    rental_id = rental_ids[0]
    customer_id = customer_ids[0]
    staff_id = staff_ids[0]
    amount = 9.99
    
    # Create records with dates in partition range
    return [{
        'customer_id': customer_id,
        'staff_id': staff_id,
        'rental_id': rental_id,
        'amount': amount,
        'payment_date': pay_date
    } for pay_date in payment_dates]

def fast_insert_chunks(engine, table_name: str, chunks: List[dict], insert_chunk_size: int):
    """Fast insert using COPY command"""
    
    # Convert chunks to DataFrame with proper column names
    df = pd.DataFrame(chunks)
    if table_name == 'inventory':
        df = df[['film_id', 'store_id', 'last_update']]
    elif table_name == 'rental':
        df = df[['rental_date', 'inventory_id', 'customer_id', 'return_date', 'staff_id', 'last_update']]
    elif table_name == 'payment':
        df = df[['customer_id', 'staff_id', 'rental_id', 'amount', 'payment_date']]

    # Create raw connection
    connection = engine.raw_connection()
    try:
        with connection.cursor() as cursor:
            # Convert DataFrame to CSV buffer
            output = StringIO()
            df.to_csv(output, sep='\t', header=False, index=False, quoting=csv.QUOTE_MINIMAL)
            output.seek(0)
            
            # Use COPY command for fastest insertion
            cursor.copy_from(
                output,
                table_name,
                columns=df.columns.tolist(),
                sep='\t'
            )
        
        connection.commit()
    except Exception as e:
        print(f"Error during bulk insert: {str(e)}")
        connection.rollback()
        raise
    finally:
        connection.close()

def optimize_for_bulk_loading(engine):
    with engine.begin() as conn:
        conn.execute(text("SET maintenance_work_mem = '1GB';"))
        conn.execute(text("SET synchronous_commit = OFF;"))

def parallel_bulk_insert_additional_data(num_records: int, host: str, user: str, 
                                       password: str, database: str, num_processes: int = None):
    """Generate and insert records as fast as possible"""
    
    if num_processes is None:
        num_processes = mp.cpu_count()
    
    print(f"Using {num_processes} processes for data generation")
    
    # Create database engine
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:5432/{database}',
        pool_size=20,
        max_overflow=40
    )

    # Get initial table sizes
    initial_sizes = get_table_sizes(engine)
    print_table_sizes(initial_sizes, "Initial Table Sizes")
    
    # Optimize PostgreSQL for bulk loading
    optimize_for_bulk_loading(engine)
    
    # Get all available IDs
    with engine.connect() as conn:
        film_ids = [conn.execute(text("SELECT film_id FROM film LIMIT 1")).scalar()]
        store_ids = [conn.execute(text("SELECT store_id FROM store LIMIT 1")).scalar()]
        customer_ids = [row[0] for row in conn.execute(text("SELECT customer_id FROM customer"))]
        staff_ids = [conn.execute(text("SELECT staff_id FROM staff LIMIT 1")).scalar()]
    
    # Use smaller chunk size to avoid memory issues
    chunk_size = 50000
    
    print(f"\nUsing chunk size: {chunk_size:,}")
    
    # Generate and insert inventory
    print("\nGenerating and inserting inventory records...")
    disable_indexes(engine, 'inventory')
    for i in tqdm(range(0, num_records, chunk_size)):
        batch_size = min(chunk_size, num_records - i)
        inventory_data = generate_inventory_chunk(batch_size, film_ids, store_ids)
        fast_insert_chunks(engine, 'inventory', inventory_data, batch_size)
    enable_indexes(engine, 'inventory')
    
    # Get all inventory IDs for rentals
    with engine.connect() as conn:
        inventory_ids = [row[0] for row in conn.execute(text("SELECT inventory_id FROM inventory"))]
      # Generate and insert rentals
    print("\nGenerating and inserting rental records...")
    disable_indexes(engine, 'rental')
    
    # Get the current maximum rental date to avoid duplicates
    with engine.connect() as conn:
        max_date = conn.execute(text("""
            SELECT COALESCE(MAX(rental_date), '2022-01-01'::timestamp)
            FROM rental
        """)).scalar() or datetime(2022, 1, 1)
    
    # Add one minute to the max_date to start from
    if isinstance(max_date, str):
        max_date = datetime.fromisoformat(max_date.replace('Z', '+00:00'))
    
    start_date = max_date + timedelta(minutes=1)
    
    for i in tqdm(range(0, num_records, chunk_size)):
        batch_size = min(chunk_size, num_records - i)
        rental_data = generate_rental_chunk(
            batch_size, 
            inventory_ids, 
            customer_ids, 
            staff_ids,
            start_date
        )
        fast_insert_chunks(engine, 'rental', rental_data, batch_size)
        # Update start_date for next batch
        start_date = start_date + timedelta(minutes=batch_size)
    
    enable_indexes(engine, 'rental')
    
    # Get rental IDs for payments
    with engine.connect() as conn:
        rental_ids = [row[0] for row in conn.execute(text("SELECT rental_id FROM rental LIMIT 1"))]
    
    # Generate and insert payments
    print("\nGenerating and inserting payment records...")
    disable_indexes(engine, 'payment')
    for i in tqdm(range(0, num_records, chunk_size)):
        batch_size = min(chunk_size, num_records - i)
        payment_data = generate_payment_chunk(batch_size, rental_ids, customer_ids, staff_ids)
        fast_insert_chunks(engine, 'payment', payment_data, batch_size)
    enable_indexes(engine, 'payment')
    
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
    
    print("\nAll operations completed successfully!")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate additional records for largest tables')
    parser.add_argument('num_records', type=int, help='Number of records to generate')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--user', default='postgres', help='PostgreSQL username')
    parser.add_argument('--password', default='postgres', help='PostgreSQL password')
    parser.add_argument('--database', default='pagila', help='PostgreSQL database name')
    parser.add_argument('--processes', type=int, help='Number of processes to use')
    args = parser.parse_args()
    
    parallel_bulk_insert_additional_data(
        args.num_records,
        args.host,
        args.user,
        args.password,
        args.database,
        args.processes
    )
