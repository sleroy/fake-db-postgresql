from faker import Faker
from sqlalchemy import create_engine, text
from datetime import datetime
import random
from typing import List
import pandas as pd
from tqdm import tqdm
import concurrent.futures
from functools import partial

fake = Faker()

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

def generate_long_description() -> str:
    """Generate a longer, more detailed description"""
    # Generate multiple paragraphs for a longer description
    paragraphs = [fake.paragraph(nb_sentences=15) for _ in range(5)]
    return '\n\n'.join(paragraphs)

def generate_plot_summary() -> str:
    """Generate a detailed plot summary"""
    elements = [
        fake.catch_phrase(),
        fake.text(max_nb_chars=200),
        f"Starring {fake.name()} and {fake.name()}",
        fake.paragraph(nb_sentences=3),
        f"Directed by {fake.name()}",
        fake.paragraph(nb_sentences=2)
    ]
    return '\n\n'.join(elements)


def generate_detailed_address() -> str:
    """Generate a detailed address with additional information"""
    parts = [
        fake.street_address(),
        f"Building: {fake.building_number()}",
        f"Block: {fake.random_letter().upper()}-{fake.random_digit()}",
        f"Zone: {fake.random_int(min=1, max=99)}",
        f"Additional Info: {fake.sentence()}"
    ]
    return ', '.join(parts)

def generate_film_chunk(chunk_size: int, language_ids: List[int]) -> List[dict]:
    """Generate a chunk of film data with enhanced text content"""
    ratings = ['G', 'PG', 'PG-13', 'R', 'NC-17']
    special_features = [
        ['Trailers', 'Commentaries', 'Deleted Scenes', 'Behind the Scenes'],
        ['Trailers', 'Commentaries', 'Behind the Scenes'],
        ['Deleted Scenes', 'Behind the Scenes'],
        ['Trailers', 'Deleted Scenes']
    ]
    
    return [{
        'title': f"{fake.catch_phrase()} {random.choice(['Chronicles', 'Story', 'Tales', 'Adventures', 'Legacy'])} - {fake.word().title()}",
        'description': generate_long_description(),  # Much longer description
        'release_year': random.randint(1970, 2023),
        'language_id': random.choice(language_ids),
        'original_language_id': random.choice(language_ids) if random.random() > 0.7 else None,
        'rental_duration': random.randint(3, 14),
        'rental_rate': round(random.uniform(0.99, 9.99), 2),
        'length': random.randint(60, 240),
        'replacement_cost': round(random.uniform(9.99, 49.99), 2),
        'rating': random.choice(ratings),
        'last_update': datetime.now(),
        'special_features': random.choice(special_features),
        'fulltext': None  # This will be automatically updated by trigger
    } for _ in range(chunk_size)]


def generate_customer_chunk(chunk_size: int, store_ids: List[int], address_ids: List[int]) -> List[dict]:
    """Generate a chunk of customer data with enhanced text fields"""
    return [{
        'store_id': random.choice(store_ids),
        'first_name': ' '.join([fake.first_name() for _ in range(random.randint(1, 3))]),  # Multiple first names
        'last_name': ' '.join([fake.last_name() for _ in range(random.randint(1, 2))]),    # Multiple last names
        'email': f"{fake.user_name()}_{fake.random_int()}@{fake.domain_name()}",
        'address_id': random.choice(address_ids),
        'activebool': True,
        'create_date': fake.date_between(start_date='-5y'),
        'last_update': datetime.now(),
        'active': 1
    } for _ in range(chunk_size)]



def generate_location_chunk(num_records: int, existing_ids: dict) -> tuple:
    """Generate location data with enhanced text content"""
    countries = [{
        'country': f"{fake.country()} {fake.country_code()}",  # Enhanced country name
        'last_update': datetime.now()
    } for _ in range(min(num_records // 10, 100))]
    
    cities = [{
        'city': f"{fake.city()} {fake.city_suffix()} {random.choice(['North', 'South', 'East', 'West'])}",  # Enhanced city name
        'country_id': random.choice(existing_ids['country_ids'] or [1]),
        'last_update': datetime.now()
    } for _ in range(min(num_records // 5, 600))]
    
    addresses = [{
        'address': generate_detailed_address(),  # Enhanced address
        'address2': f"Suite {fake.building_number()}, Floor {random.randint(1,50)}, {fake.secondary_address()}" if random.random() > 0.3 else None,
        'district': f"{fake.city()} District {random.randint(1,99)}",
        'city_id': random.choice(existing_ids['city_ids'] or [1]),
        'postal_code': f"{fake.postcode()}-{fake.postcode()}",
        'phone': f"{fake.phone_number()} / {fake.phone_number()}", # Multiple phone numbers
        'last_update': datetime.now()
    } for _ in range(min(num_records // 2, 1000))]
    
    return countries, cities, addresses



def parallel_generate_data(func, num_records: int, chunk_size: int, **kwargs) -> List[dict]:
    """Generate data in parallel"""
    chunks = [(chunk_size if i < num_records - chunk_size else num_records - i) 
             for i in range(0, num_records, chunk_size)]
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        partial_func = partial(func, **kwargs)
        results = list(tqdm(
            executor.map(partial_func, chunks),
            total=len(chunks),
            desc=f"Generating {func.__name__.replace('generate_', '')}"
        ))
    
    return [item for sublist in results for item in sublist]

def generate_staff_data(address_id: int, store_id: int) -> dict:
    """Generate enhanced staff data"""
    return {
        'first_name': ' '.join([fake.first_name() for _ in range(random.randint(1, 2))]),
        'last_name': ' '.join([fake.last_name() for _ in range(random.randint(1, 2))]),
        'address_id': address_id,
        'email': f"{fake.user_name()}_{fake.random_int()}@{fake.domain_name()}",
        'store_id': store_id,
        'active': True,
        'username': fake.user_name(),
        'password': fake.password(length=random.randint(12, 30)),
        'last_update': datetime.now(),
        'picture': None  # Keep as None or implement proper binary data if needed
    }

def bulk_insert_data(num_records: int, host: str, user: str, password: str, database: str):
    """Main function to handle data insertion"""
    print(f"Connecting to database {database} on {host}...")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:5432/{database}')
    
    # Get or create base data
    existing_ids = create_base_data(engine)
    
    # Generate and insert location data if needed
    if not existing_ids['country_ids'] or not existing_ids['city_ids'] or not existing_ids['address_ids']:
        countries, cities, addresses = generate_location_chunk(num_records, existing_ids)
        
        if not existing_ids['country_ids']:
            pd.DataFrame(countries).to_sql('country', engine, if_exists='append', index=False)
        if not existing_ids['city_ids']:
            pd.DataFrame(cities).to_sql('city', engine, if_exists='append', index=False)
        if not existing_ids['address_ids']:
            pd.DataFrame(addresses).to_sql('address', engine, if_exists='append', index=False)
        
        # Refresh IDs
        existing_ids = create_base_data(engine)
    
    # Create initial structure (stores and staff)
    create_initial_structure(engine, existing_ids['address_ids'])
    
    # Get store IDs
    with engine.connect() as conn:
        store_ids = [row[0] for row in conn.execute(text("SELECT store_id FROM store"))]
    
    # Generate and insert customers and films in parallel
    chunk_size = 10000
    
    print("Generating and inserting customers...")
    customers = parallel_generate_data(
        generate_customer_chunk, 
        num_records, 
        chunk_size,
        store_ids=store_ids,
        address_ids=existing_ids['address_ids']
    )
    pd.DataFrame(customers).to_sql('customer', engine, if_exists='append', index=False, method='multi', chunksize=chunk_size)
    
    print("Generating and inserting films...")
    films = parallel_generate_data(
        generate_film_chunk,
        num_records,
        chunk_size,
        language_ids=existing_ids['language_ids']
    )
    pd.DataFrame(films).to_sql('film', engine, if_exists='append', index=False, method='multi', chunksize=chunk_size)
    
    # Reset sequences
    with engine.connect() as conn:
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
        conn.commit()
    
    print("Data generation and insertion complete!")

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
