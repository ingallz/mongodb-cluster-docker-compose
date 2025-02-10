from pymongo import MongoClient
import random
import time
from faker import Faker
from datetime import datetime
from tqdm import tqdm
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import math

def connect_to_mongodb():
    try:
        client = MongoClient('mongodb://localhost:27117,localhost:27118')
        db = client['MyDatabase']
        collection = db['MyCollection']
        client.admin.command('ping')
        return collection
    except Exception as e:
        print(f"Connection error: {e}")
        return None

def generate_batch_data(batch_size):
    fake = Faker(['en_US'])
    batch_data = []
    
    categories = ['Electronics', 'Clothing', 'Food', 'Tools', 'Books']
    
    for _ in range(batch_size):
        data = {
            'oemNumber': str(random.randint(1000000, 9999999)),
            'zipCode': fake.postcode(),
            'supplierId': str(random.randint(1000, 9999)),
            'productName': fake.word() + " " + fake.word(),
            'price': round(random.uniform(10.0, 1000.0), 2),
            'quantity': random.randint(1, 1000),
            'createdAt': datetime.now(),
            'description': fake.text(max_nb_chars=200),
            'manufacturer': fake.company(),
            'category': random.choice(categories)
        }
        batch_data.append(data)
    return batch_data

def import_batch(args):
    batch_size, mongo_uri = args
    try:
        client = MongoClient(mongo_uri)
        collection = client['MyDatabase']['MyCollection']
        
        batch_data = generate_batch_data(batch_size)
        collection.insert_many(batch_data)
        
        client.close()
        return len(batch_data)
    except Exception as e:
        print(f"Process error: {e}")
        return 0

def parallel_import(num_records, batch_size=5000):
    num_processes = mp.cpu_count()
    num_batches = math.ceil(num_records / batch_size)
    
    mongo_uri = 'mongodb://localhost:27117,localhost:27118'
    args_list = [(batch_size, mongo_uri) for _ in range(num_batches)]
    
    print(f"Using {num_processes} processes")
    
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        with tqdm(total=num_records, desc="Importing records") as pbar:
            for result in executor.map(import_batch, args_list):
                pbar.update(result)

def main():
    NUM_RECORDS = 1000000  # 1 million records
    BATCH_SIZE = 5000

    start_time = time.time()
    
    collection = connect_to_mongodb()
    if collection is not None:
        print("Starting data import...")
        parallel_import(NUM_RECORDS, BATCH_SIZE)
        
        end_time = time.time()
        print(f"Execution time: {round(end_time - start_time, 2)} seconds")
    else:
        print("Could not connect to MongoDB")

if __name__ == "__main__":
    main()
