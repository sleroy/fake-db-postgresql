from faker import Faker
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import random
from typing import List, Dict
import pandas as pd
from tqdm import tqdm

fake = Faker()

def get_or_create_languages(engine) -> List[int]:
    """Get existing language IDs or create default languages if needed"""
    default_languages = [
        {'name': 'English', 'last_update': datetime.now()},
        {'name': 'Italian', 'last_update': datetime.now()},
        {'name': 'Japanese', 'last_update': datetime.now()},
        {'name': 'Mandarin', 'last_update': datetime.now()},
        {'name': 'French', 'last_update': datetime.now()},
        {'name': 'German', 'last_update': datetime.now()}
    ]
    
    with engine.connect() as conn:
        print("Checking languages...")
        result = conn.execute(text("SELECT language_id FROM language"))
        language_ids = [row[0] for row in result]
        
        if not language_ids:
            print("Creating default languages...")
            df_languages = pd.DataFrame(default_languages)
            df_languages.to_sql('language', engine, if_exists='append', index=False, method='multi', chunksize=1000)
            
            result = conn.execute(text("SELECT language_id FROM language"))
            language_ids = [row[0] for row in result]
    
    return language_ids

def get_or_create_countries(engine, num_records: int) -> List[int]:
    """Get existing country IDs or create new ones if needed"""
    with engine.connect() as conn:
        print("Checking countries...")
        result = conn.execute(text("SELECT country_id FROM country"))
        country_ids = [row[0] for row in result]
        
        if not country_ids:
            print("Creating countries...")
            countries = []
            for _ in tqdm(range(min(num_records, 100)), desc="Generating countries"):
                countries.append({
                    'country': fake.country(),
                    'last_update': datetime.now()
                })
            df_countries = pd.DataFrame(countries)
            df_countries.to_sql('country', engine, if_exists='append', index=False, method='multi', chunksize=1000)
            
            result = conn.execute(text("SELECT country_id FROM country"))
            country_ids = [row[0] for row in result]
    
    return country_ids

def get_or_create_cities(engine, num_records: int, country_ids: List[int]) -> List[int]:
    """Get existing city IDs or create new ones if needed"""
    with engine.connect() as conn:
        print("Checking cities...")
        result = conn.execute(text("SELECT city_id FROM city"))
        city_ids = [row[0] for row in result]
        
        if not city_ids:
            print("Creating cities...")
            cities = []
            for _ in tqdm(range(min(num_records, 600)), desc="Generating cities"):
                cities.append({
                    'city': fake.city(),
                    'country_id': random.choice(country_ids),
                    'last_update': datetime.now()
                })
            df_cities = pd.DataFrame(cities)
            df_cities.to_sql('city', engine, if_exists='append', index=False, method='multi', chunksize=1000)
            
            result = conn.execute(text("SELECT city_id FROM city"))
            city_ids = [row[0] for row in result]
    
    return city_ids

def get_or_create_addresses(engine, num_records: int, city_ids: List[int]) -> List[int]:
    """Get existing address IDs or create new ones if needed"""
    with engine.connect() as conn:
        print("Checking addresses...")
        result = conn.execute(text("SELECT address_id FROM address"))
        address_ids = [row[0] for row in result]
        
        if not address_ids or len(address_ids) < 10:
            print("Creating addresses...")
            addresses = []
            for _ in tqdm(range(max(10, min(num_records, 1000))), desc="Generating addresses"):
                addresses.append({
                    'address': fake.street_address(),
                    'address2': fake.secondary_address() if random.random() > 0.7 else None,
                    'district': fake.city(),
                    'city_id': random.choice(city_ids),
                    'postal_code': fake.zipcode(),
                    'phone': fake.phone_number(),
                    'last_update': datetime.now()
                })
            df_addresses = pd.DataFrame(addresses)
            df_addresses.to_sql('address', engine, if_exists='append', index=False, method='multi', chunksize=1000)
            
            result = conn.execute(text("SELECT address_id FROM address"))
            address_ids = [row[0] for row in result]
    
    return address_ids

def get_or_create_staff(engine, address_ids: List[int]) -> List[int]:
    """Get existing staff IDs or create minimum required staff"""
    with engine.connect() as conn:
        print("Checking staff...")
        result = conn.execute(text("SELECT staff_id, store_id FROM staff"))
        staff_data = [(row[0], row[1]) for row in result]
        
        if not staff_data or len(staff_data) < 2:
            print("Creating staff members...")
            staff = []
            for i in tqdm(range(2), desc="Generating staff"):
                staff.append({
                    'first_name': fake.first_name(),
                    'last_name': fake.last_name(),
                    'address_id': random.choice(address_ids),
                    'email': fake.email(),
                    'active': True,
                    'username': fake.user_name(),
                    'password': fake.password(),
                    'store_id': 1,
                    'last_update': datetime.now()
                })
            df_staff = pd.DataFrame(staff)
            df_staff.to_sql('staff', engine, if_exists='append', index=False, method='multi', chunksize=1000)
            
            result = conn.execute(text("SELECT staff_id FROM staff"))
            staff_ids = [row[0] for row in result]
        else:
            staff_ids = [row[0] for row in staff_data]
    
    return staff_ids

def get_or_create_stores(engine, staff_ids: List[int], address_ids: List[int]) -> List[int]:
    """Get existing store IDs or create minimum required stores"""
    with engine.connect() as conn:
        print("Checking stores...")
        result = conn.execute(text("SELECT store_id FROM store"))
        store_ids = [row[0] for row in result]
        
        if not store_ids:
            print("Creating stores...")
            # Create store 1
            conn.execute(text("""
                INSERT INTO store (store_id, manager_staff_id, address_id, last_update)
                VALUES (1, :staff_id, :address_id, NOW())
            """), {
                "staff_id": staff_ids[0],
                "address_id": address_ids[0]
            })
            
            if len(staff_ids) >= 2:
                conn.execute(text("""
                    INSERT INTO store (store_id, manager_staff_id, address_id, last_update)
                    VALUES (2, :staff_id, :address_id, NOW())
                """), {
                    "staff_id": staff_ids[1],
                    "address_id": address_ids[1]
                })
            conn.commit()
            
            result = conn.execute(text("SELECT store_id FROM store"))
            store_ids = [row[0] for row in result]
    
    return store_ids

def create_customers(num_records: int, address_ids: List[int], store_ids: List[int]) -> List[dict]:
    """Create new customers"""
    customers = []
    for _ in tqdm(range(num_records), desc="Generating customers"):
        customers.append({
            'store_id': random.choice(store_ids),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'address_id': random.choice(address_ids),
            'activebool': True,
            'create_date': fake.date_between(start_date='-1y'),
            'last_update': datetime.now(),
            'active': 1
        })
    return customers

def create_films(num_records: int, language_ids: List[int]) -> List[dict]:
    """Create new films"""
    ratings = ['G', 'PG', 'PG-13', 'R', 'NC-17']
    special_features = [
        ['Trailers', 'Commentaries'],
        ['Deleted Scenes'],
        ['Behind the Scenes'],
        ['Trailers', 'Deleted Scenes', 'Behind the Scenes']
    ]
    
    films = []
    for _ in tqdm(range(num_records), desc="Generating films"):
        films.append({
            'title': fake.catch_phrase(),
            'description': fake.text(max_nb_chars=200),
            'release_year': random.randint(1970, 2023),
            'language_id': random.choice(language_ids),
            'rental_duration': random.randint(3, 7),
            'rental_rate': round(random.uniform(0.99, 4.99), 2),
            'length': random.randint(60, 180),
            'replacement_cost': round(random.uniform(9.99, 29.99), 2),
            'rating': random.choice(ratings),
            'last_update': datetime.now(),
            'special_features': random.choice(special_features)
        })
    return films

def bulk_insert_data(num_records: int, host: str, user: str, password: str, database: str):
    """Main function to handle data insertion"""
    print(f"Connecting to database {database} on {host}...")
    db_connection = f'postgresql://{user}:{password}@{host}:5432/{database}'
    engine = create_engine(db_connection)
    
    # Get or create the minimum required data for the hierarchy
    language_ids = get_or_create_languages(engine)
    country_ids = get_or_create_countries(engine, num_records)
    city_ids = get_or_create_cities(engine, num_records, country_ids)
    address_ids = get_or_create_addresses(engine, num_records, city_ids)
    staff_ids = get_or_create_staff(engine, address_ids)
    store_ids = get_or_create_stores(engine, staff_ids, address_ids)
    
    # Create and insert new customers
    print("Creating customers...")
    customers = create_customers(num_records, address_ids, store_ids)
    df_customers = pd.DataFrame(customers)
    print("Inserting customers into database...")
    df_customers.to_sql('customer', engine, if_exists='append', index=False, method='multi', chunksize=1000)
    
    # Create and insert new films
    print("Creating films...")
    films = create_films(num_records, language_ids)
    df_films = pd.DataFrame(films)
    print("Inserting films into database...")
    df_films.to_sql('film', engine, if_exists='append', index=False, method='multi', chunksize=1000)

    print("Resetting sequences...")
    with engine.connect() as conn:
        conn.execute(text("""
            SELECT setval('language_language_id_seq', (SELECT MAX(language_id) FROM language));
            SELECT setval('store_store_id_seq', (SELECT MAX(store_id) FROM store));
            SELECT setval('staff_staff_id_seq', (SELECT MAX(staff_id) FROM staff));
            SELECT setval('customer_customer_id_seq', (SELECT MAX(customer_id) FROM customer));
            SELECT setval('film_film_id_seq', (SELECT MAX(film_id) FROM film));
            SELECT setval('address_address_id_seq', (SELECT MAX(address_id) FROM address));
            SELECT setval('city_city_id_seq', (SELECT MAX(city_id) FROM city));
            SELECT setval('country_country_id_seq', (SELECT MAX(country_id) FROM country));
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
