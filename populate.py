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

def generate_cached_values() -> Dict:
    """Pre-generate common values to avoid repeated random generation"""
    return {
        'first_names': [fake.first_name() for _ in range(100)],
        'last_names': [fake.last_name() for _ in range(100)],
        'email_domains': [fake.domain_name() for _ in range(20)],
        'street_types': ['Street', 'Avenue', 'Road', 'Boulevard', 'Lane', 'Drive'],
        'cities': [fake.city() for _ in range(50)],
        'descriptions': ['\n'.join([fake.paragraph(nb_sentences=10) for _ in range(3)]) for _ in range(50)],
        'titles': [fake.catch_phrase() for _ in range(100)],
        'phone_formats': ["(###) ###-####", "###-###-####", "+1 ### ### ####"],
        'special_features_options': [
            ['Trailers', 'Commentaries', 'Deleted Scenes', 'Behind the Scenes'],
            ['Trailers', 'Commentaries'],
            ['Deleted Scenes', 'Behind the Scenes'],
            ['Trailers', 'Behind the Scenes']
        ]
    }

def create_base_data(engine) -> dict:
    """Create or get all base data needed for the database"""
    with engine.connect() as conn:
        # Default languages
        languages = [
            {'name': lang, 'last_update': datetime.now()} 
            for lang in ['English', 'Italian', 'Japanese', 'Mandarin', 'French', 'German']
        ]
        
        # Check and create languages
        result = conn.execute(text("SELECT language_id FROM language"))
        if not result.fetchall():
            pd.DataFrame(languages).to_sql('language', engine, if_exists='append', index=False)
        
        # Get all necessary IDs
        language_ids = [row[0] for row in conn.execute(text("SELECT language_id FROM language"))]
        country_ids = [row[0] for row in conn.execute(text("SELECT country_id FROM country"))]
        city_ids = [row[0] for row in conn.execute(text("SELECT city_id FROM city"))]
        address_ids = [row[0] for row in conn.execute(text("SELECT address_id FROM address"))]
        
        return {
            'language_ids': language_ids,
            'country_ids': country_ids,
            'city_ids': city_ids,
            'address_ids': address_ids
        }

def create_initial_structure(engine, address_ids: List[int]):
    """Create initial stores and staff if they don't exist"""
    with engine.connect() as conn:
        # Check existing data
        stores = [row[0] for row in conn.execute(text("SELECT store_id FROM store"))]
        staff = [row[0] for row in conn.execute(text("SELECT staff_id FROM staff"))]
        
        if not stores or not staff:
            # Create first store and staff member
            conn.execute(text("""
                INSERT INTO store (store_id, manager_staff_id, address_id, last_update)
                VALUES (1, 1, :address_id, NOW())
            """), {"address_id": address_ids[0]})
            
            conn.execute(text("""
                INSERT INTO staff (first_name, last_name, address_id, email, active, username, password, store_id, last_update)
                VALUES (:fname, :lname, :addr_id, :email, TRUE, :uname, :pwd, 1, NOW())
            """), {
                "fname": fake.first_name(), "lname": fake.last_name(),
                "addr_id": address_ids[0], "email": fake.email(),
                "uname": fake.user_name(), "pwd": fake.password()
            })
            
            # Update first store with correct manager
            staff_id = conn.execute(text("SELECT staff_id FROM staff ORDER BY staff_id DESC LIMIT 1")).scalar()
            conn.execute(text("UPDATE store SET manager_staff_id = :staff_id WHERE store_id = 1"), 
                        {"staff_id": staff_id})
            
            # Create second store and staff member
            conn.execute(text("""
                INSERT INTO store (store_id, manager_staff_id, address_id, last_update)
                VALUES (2, :staff_id, :address_id, NOW())
            """), {"staff_id": staff_id, "address_id": address_ids[1]})
            
            conn.execute(text("""
                INSERT INTO staff (first_name, last_name, address_id, email, active, username, password, store_id, last_update)
                VALUES (:fname, :lname, :addr_id, :email, TRUE, :uname, :pwd, 2, NOW())
            """), {
                "fname": fake.first_name(), "lname": fake.last_name(),
                "addr_id": address_ids[1], "email": fake.email(),
                "uname": fake.user_name(), "pwd": fake.password()
            })
            
            conn.commit()

def fast_generate_film_chunk(chunk_size: int, language_ids: List[int], cached_values: dict) -> List[dict]:
    """Generate film data using cached values for better performance"""
    ratings = ['G', 'PG', 'PG-13', 'R', 'NC-17']
    current_time = datetime.now()
    
    return [{
        'title': f"{random.choice(cached_values['titles'])} {random.choice(['Chronicles', 'Story', 'Tales', 'Adventures', 'Legacy'])}",
        'description': random.choice(cached_values['descriptions']),
        'release_year': random.randint(1970, 2023),
        'language_id': random.choice(language_ids),
        'original_language_id': random.choice(language_ids) if random.random() > 0.7 else None,
        'rental_duration': random.randint(3, 14),
        'rental_rate': round(random.uniform(0.99, 9.99), 2),
        'length': random.randint(60, 240),
        'replacement_cost': round(random.uniform(9.99, 49.99), 2),
        'rating': random.choice(ratings),
        'last_update': current_time,
        'special_features': random.choice(cached_values['special_features_options']),
        'fulltext': None
    } for _ in range(chunk_size)]

def fast_generate_customer_chunk(chunk_size: int, store_ids: List[int], address_ids: List[int], cached_values: dict) -> List[dict]:
    """Generate customer data using cached values for better performance"""
    current_time = datetime.now()
    
    return [{
        'store_id': random.choice(store_ids),
        'first_name': random.choice(cached_values['first_names']),
        'last_name': random.choice(cached_values['last_names']),
        'email': f"{random.choice(cached_values['first_names']).lower()}.{random.choice(cached_values['last_names']).lower()}@{random.choice(cached_values['email_domains'])}",
        'address_id': random.choice(address_ids),
        'activebool': True,
        'create_date': current_time.date(),
        'last_update': current_time,
        'active': 1
    } for _ in range(chunk_size)]

def fast_generate_location_chunk(num_records: int, existing_ids: dict, cached_values: dict) -> tuple:
    """Generate location data using cached values for better performance"""
    current_time = datetime.now()
    
    countries = [{
        'country': f"{fake.country()}",
        'last_update': current_time
    } for _ in range(min(num_records // 10, 100))]
    
    cities = [{
        'city': random.choice(cached_values['cities']),
        'country_id': random.choice(existing_ids['country_ids'] or [1]),
        'last_update': current_time
    } for _ in range(min(num_records // 5, 600))]
    
    addresses = [{
        'address': f"{random.randint(1, 9999)} {random.choice(cached_values['cities'])} {random.choice(cached_values['street_types'])}",
        'address2': None if random.random() > 0.3 else f"Apt {random.randint(1, 999)}",
        'district': random.choice(cached_values['cities']),
        'city_id': random.choice(existing_ids['city_ids'] or [1]),
        'postal_code': str(random.randint(10000, 99999)),
        'phone': random.choice(cached_values['phone_formats']).replace('#', lambda _: str(random.randint(0, 9))),
        'last_update': current_time
    } for _ in range(min(num_records // 2, 1000))]
    
    return countries, cities, addresses

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

def bulk_insert_data(num_records: int, host: str, user: str, password: str, database: str):
    """Main function to handle data insertion with dynamic chunk sizing"""
    print(f"Connecting to database {database} on {host}...")
    
    # Calculate optimal chunk sizes
    processing_chunk_size = calculate_optimal_chunk_size(num_records)
    insert_chunk_size = calculate_insert_chunk_size(processing_chunk_size)
    
    print(f"Using processing chunk size: {processing_chunk_size:,}")
    print(f"Using INSERT chunk size: {insert_chunk_size:,}")
    
    # Initialize cached values
    print("Initializing cached values...")
    cached_values = generate_cached_values()
    
    # Adjust pool size based on data volume
    pool_size = min(20, max(5, num_records // 100_000))
    max_overflow = pool_size * 2
    
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:5432/{database}',
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=30
    )
    
    # Get or create base data
    existing_ids = create_base_data(engine)
    
    # Generate and insert location data if needed
    if not existing_ids['country_ids'] or not existing_ids['city_ids'] or not existing_ids['address_ids']:
        countries, cities, addresses = fast_generate_location_chunk(num_records, existing_ids, cached_values)
        
        with engine.begin() as conn:
            if not existing_ids['country_ids']:
                print("Inserting countries...")
                pd.DataFrame(countries).to_sql('country', conn, if_exists='append', 
                                             index=False, method='multi', 
                                             chunksize=insert_chunk_size)
            
            if not existing_ids['city_ids']:
                print("Inserting cities...")
                pd.DataFrame(cities).to_sql('city', conn, if_exists='append', 
                                          index=False, method='multi', 
                                          chunksize=insert_chunk_size)
            
            if not existing_ids['address_ids']:
                print("Inserting addresses...")
                pd.DataFrame(addresses).to_sql('address', conn, if_exists='append', 
                                             index=False, method='multi', 
                                             chunksize=insert_chunk_size)
        
        # Refresh IDs after insertion
        existing_ids = create_base_data(engine)
    
    # Create initial structure (stores and staff)
    create_initial_structure(engine, existing_ids['address_ids'])
    
    # Get store IDs
    with engine.connect() as conn:
        store_ids = [row[0] for row in conn.execute(text("SELECT store_id FROM store"))]
    
    # Generate and insert data in optimized chunks
    total_chunks = (num_records + processing_chunk_size - 1) // processing_chunk_size
    
    print(f"\nInserting {num_records:,} records in {total_chunks:,} chunks")
    print(f"Estimated total batches: {(num_records // insert_chunk_size):,}")
    
    # Generate customers and films in chunks
    print("\nGenerating and inserting customers...")
    for offset in tqdm(range(0, num_records, processing_chunk_size)):
        current_chunk_size = min(processing_chunk_size, num_records - offset)
        customers = fast_generate_customer_chunk(
            current_chunk_size,
            store_ids=store_ids,
            address_ids=existing_ids['address_ids'],
            cached_values=cached_values
        )
        
        with engine.begin() as conn:
            pd.DataFrame(customers).to_sql(
                'customer', 
                conn, 
                if_exists='append',
                index=False,
                method='multi',
                chunksize=insert_chunk_size
            )
    
    print("\nGenerating and inserting films...")
    for offset in tqdm(range(0, num_records, processing_chunk_size)):
        current_chunk_size = min(processing_chunk_size, num_records - offset)
        films = fast_generate_film_chunk(
            current_chunk_size,
            language_ids=existing_ids['language_ids'],
            cached_values=cached_values
        )
        
        with engine.begin() as conn:
            pd.DataFrame(films).to_sql(
                'film',
                conn,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=insert_chunk_size
            )
    
    # Reset sequences
    print("\nResetting sequences...")
    with engine.begin() as conn:
        conn.execute(text("""
            SELECT setval(pg_get_serial_sequence('language', 'language_id'), (SELECT MAX(language_id) FROM language));
            SELECT setval(pg_get_serial_sequence('store', 'store_id'), (SELECT MAX(store_id) FROM store));
            SELECT setval(pg_get_serial_sequence('staff', 'staff_id'), (SELECT MAX(staff_id) FROM staff));
            SELECT setval(pg_get_serial_sequence('customer', 'customer_id'), (SELECT MAX(customer_id) FROM customer));
            SELECT setval(pg_get_serial_sequence('film', 'film_id'), (SELECT MAX(film_id) FROM film));
            SELECT setval(pg_get_serial_sequence('address', 'address_id'), (SELECT MAX(address_id) FROM address));
            SELECT setval(pg_get_serial_sequence('city', 'city_id'), (SELECT MAX(city_id) FROM city));
            SELECT setval(pg_get_serial_sequence('country', 'country_id'), (SELECT MAX(country_id) FROM country));
        """))
    
    print("\nData generation and insertion complete!")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate fake data for Pagila database')
    parser.add_argument('num_records', type=int, help='Number of records to generate')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--user', default='postgres', help='PostgreSQL username')
    parser.add_argument('--password', default='postgres', help='PostgreSQL password')
    parser.add_argument('--database', default='pagila', help='PostgreSQL database name')
    args = parser.parse_args()
    
    bulk_insert_data(args.num_records, args.host, args.user, args.password, args.database)
