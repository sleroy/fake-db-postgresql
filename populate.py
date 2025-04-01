from faker import Faker
from sqlalchemy import create_engine, text
from datetime import datetime
import random
from typing import List, Dict
import pandas as pd
from tqdm import tqdm
import concurrent.futures
from functools import partial

fake = Faker()

def get_existing_film_actor_relationships(engine) -> set:
    """Get existing film-actor relationships"""
    with engine.connect() as conn:
        results = conn.execute(text("SELECT film_id, actor_id FROM film_actor"))
        return {(row[0], row[1]) for row in results}

def get_table_sizes(engine) -> List[tuple]:
    """Get table sizes ordered by number of records"""
    query = """
    SELECT 
        relname as table_name,
        n_live_tup as row_count,
        pg_size_pretty(pg_total_relation_size(relid)) as total_size,
        pg_size_pretty(pg_relation_size(relid)) as table_size,
        pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) as index_size
    FROM pg_stat_user_tables
    ORDER BY n_live_tup DESC;
    """
    
    with engine.connect() as conn:
        results = conn.execute(text(query))
        return results.fetchall()

def generate_inventory_chunk(chunk_size: int, film_ids: List[int], store_ids: List[int]) -> List[dict]:
    """Generate inventory records"""
    current_time = datetime.now()
    
    return [{
        'film_id': random.choice(film_ids),
        'store_id': random.choice(store_ids),
        'last_update': current_time
    } for _ in range(chunk_size)]

def generate_payment_chunk(chunk_size: int, rental_ids: List[int], 
                         customer_ids: List[int], staff_ids: List[int]) -> List[dict]:
    """Generate payment records within the valid partition range (2022 only)"""
    # Define date range for 2022 only
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2022, 7, 31)  # Last available partition is 2022-07
    
    return [{
        'customer_id': random.choice(customer_ids),
        'staff_id': random.choice(staff_ids),
        'rental_id': random.choice(rental_ids),
        'amount': round(random.uniform(0.99, 9.99), 2),
        'payment_date': fake.date_time_between(start_date=start_date, end_date=end_date)
    } for _ in range(chunk_size)]

def generate_rental_chunk(chunk_size: int, inventory_ids: List[int], 
                         customer_ids: List[int], staff_ids: List[int]) -> List[dict]:
    """Generate rental records with dates matching payment dates"""
    # Use same date range as payments
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

def generate_actor_chunk(chunk_size: int) -> List[dict]:
    """Generate actor records"""
    current_time = datetime.now()
    
    return [{
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'last_update': current_time
    } for _ in range(chunk_size)]

def generate_film_actor_chunk(chunk_size: int, film_ids: List[int], actor_ids: List[int], 
                            existing_combinations: set) -> List[dict]:
    """Generate film_actor records with unique combinations"""
    if not film_ids or not actor_ids:
        raise ValueError("Both film_ids and actor_ids must not be empty")
        
    current_time = datetime.now()
    records = []
    
    # Limit attempts to avoid infinite loop
    max_attempts = chunk_size * 10
    attempts = 0
    
    while len(records) < chunk_size and attempts < max_attempts:
        film_id = random.choice(film_ids)
        actor_id = random.choice(actor_ids)
        
        if (film_id, actor_id) not in existing_combinations:
            existing_combinations.add((film_id, actor_id))
            records.append({
                'film_id': film_id,
                'actor_id': actor_id,
                'last_update': current_time
            })
        attempts += 1
        
        if attempts == max_attempts:
            print(f"Warning: Could only generate {len(records)} unique film-actor relationships")
            break
    
    return records


def calculate_optimal_chunk_size(total_records: int) -> int:
    """Calculate optimal chunk size based on total records"""
    if total_records < 10_000:
        chunk_size = 1_000
    elif total_records < 100_000:
        chunk_size = 5_000
    elif total_records < 1_000_000:
        chunk_size = 10_000
    elif total_records < 10_000_000:
        chunk_size = 50_000
    else:
        chunk_size = 100_000
    
    return min(chunk_size, max(1000, total_records // 10))

def calculate_insert_chunk_size(processing_chunk_size: int) -> int:
    """Calculate the INSERT chunk size based on the processing chunk size"""
    return min(5000, max(100, processing_chunk_size // 10))

def bulk_insert_additional_data(num_records: int, host: str, user: str, password: str, database: str):
    """Generate additional records for the largest tables"""
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:5432/{database}',
        pool_size=20,
        max_overflow=40,
        pool_timeout=30
    )
    
    # First, let's print current table sizes
    print("Current table sizes:")
    for table in get_table_sizes(engine):
        print(f"{table.table_name}: {table.row_count:,} rows ({table.total_size})")
    
    # Get necessary IDs
    with engine.connect() as conn:
        film_ids = [row[0] for row in conn.execute(text("SELECT film_id FROM film"))]
        store_ids = [row[0] for row in conn.execute(text("SELECT store_id FROM store"))]
        customer_ids = [row[0] for row in conn.execute(text("SELECT customer_id FROM customer"))]
        staff_ids = [row[0] for row in conn.execute(text("SELECT staff_id FROM staff"))]
        actor_ids = [row[0] for row in conn.execute(text("SELECT actor_id FROM actor"))]
    
    # Generate actors if none exist
    if not actor_ids:
        print("\nGenerating initial actors...")
        actor_records = generate_actor_chunk(100)  # Create 100 initial actors
        
        with engine.begin() as conn:
            pd.DataFrame(actor_records).to_sql(
                'actor',
                conn,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=100
            )
            
        # Refresh actor IDs
        with engine.connect() as conn:
            actor_ids = [row[0] for row in conn.execute(text("SELECT actor_id FROM actor"))]
    
    # Calculate chunk sizes
    processing_chunk_size = calculate_optimal_chunk_size(num_records)
    insert_chunk_size = calculate_insert_chunk_size(processing_chunk_size)
    
    print(f"\nGenerating additional records with chunk size: {processing_chunk_size:,}")
    
    # Generate inventory first
    print("\nGenerating inventory records...")
    for offset in tqdm(range(0, num_records, processing_chunk_size)):
        current_chunk_size = min(processing_chunk_size, num_records - offset)
        inventory_records = generate_inventory_chunk(
            current_chunk_size,
            film_ids=film_ids,
            store_ids=store_ids
        )
        
        with engine.begin() as conn:
            pd.DataFrame(inventory_records).to_sql(
                'inventory',
                conn,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=insert_chunk_size
            )
    
    # Get inventory IDs for rentals
    with engine.connect() as conn:
        inventory_ids = [row[0] for row in conn.execute(text("SELECT inventory_id FROM inventory"))]
    
    # Generate rentals
    print("\nGenerating rental records...")
    for offset in tqdm(range(0, num_records, processing_chunk_size)):
        current_chunk_size = min(processing_chunk_size, num_records - offset)
        rental_records = generate_rental_chunk(
            current_chunk_size,
            inventory_ids=inventory_ids,
            customer_ids=customer_ids,
            staff_ids=staff_ids
        )
        
        with engine.begin() as conn:
            pd.DataFrame(rental_records).to_sql(
                'rental',
                conn,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=insert_chunk_size
            )
    
    # Get rental IDs for payments
    with engine.connect() as conn:
        rental_ids = [row[0] for row in conn.execute(text("SELECT rental_id FROM rental"))]
    
    # Generate payments
    print("\nGenerating payment records...")
    for offset in tqdm(range(0, num_records, processing_chunk_size)):
        current_chunk_size = min(processing_chunk_size, num_records - offset)
        payment_records = generate_payment_chunk(
            current_chunk_size,
            rental_ids=rental_ids,
            customer_ids=customer_ids,
            staff_ids=staff_ids
        )
        
        with engine.begin() as conn:
            pd.DataFrame(payment_records).to_sql(
                'payment',
                conn,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=insert_chunk_size
            )
    
 # Generate film_actor records
    print("\nGenerating film_actor records...")
    existing_combinations = get_existing_film_actor_relationships(engine)
    print(f"Found {len(existing_combinations):,} existing film-actor relationships")
    
    # Calculate reasonable number of new relationships to add
    total_possible_combinations = len(film_ids) * len(actor_ids)
    remaining_combinations = total_possible_combinations - len(existing_combinations)
    film_actor_chunk_size = min(
        remaining_combinations,  # Don't try to add more than possible
        num_records,            # Don't exceed requested number
        100000                  # Maximum limit
    )
    
    if film_actor_chunk_size <= 0:
        print("No more unique film-actor combinations possible")
    else:
        print(f"Attempting to generate {film_actor_chunk_size:,} new film-actor relationships")
        try:
            film_actor_records = generate_film_actor_chunk(
                film_actor_chunk_size,
                film_ids=film_ids,
                actor_ids=actor_ids,
                existing_combinations=existing_combinations
            )
            
            if film_actor_records:  # Only insert if we have records
                with engine.begin() as conn:
                    pd.DataFrame(film_actor_records).to_sql(
                        'film_actor',
                        conn,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=insert_chunk_size
                    )
                print(f"Successfully added {len(film_actor_records):,} new film-actor relationships")
            else:
                print("No new film-actor relationships were generated")
        except ValueError as e:
            print(f"Warning: Could not generate film_actor records: {str(e)}")
    
    # Reset sequences
    print("\nResetting sequences...")
    with engine.begin() as conn:
        conn.execute(text("""
            SELECT setval(pg_get_serial_sequence('inventory', 'inventory_id'), (SELECT MAX(inventory_id) FROM inventory));
            SELECT setval(pg_get_serial_sequence('rental', 'rental_id'), (SELECT MAX(rental_id) FROM rental));
            SELECT setval(pg_get_serial_sequence('payment', 'payment_id'), (SELECT MAX(payment_id) FROM payment));
        """))
    
    # Print final table sizes
    print("\nFinal table sizes:")
    for table in get_table_sizes(engine):
        print(f"{table.table_name}: {table.row_count:,} rows ({table.total_size})")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate additional records for largest tables in Pagila database')
    parser.add_argument('num_records', type=int, help='Number of records to generate')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--user', default='postgres', help='PostgreSQL username')
    parser.add_argument('--password', default='postgres', help='PostgreSQL password')
    parser.add_argument('--database', default='pagila', help='PostgreSQL database name')
    args = parser.parse_args()
    
    bulk_insert_additional_data(args.num_records, args.host, args.user, args.password, args.database)
