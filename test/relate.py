from pymongo import MongoClient
import random
import time
from faker import Faker
from datetime import datetime, timedelta
from tqdm import tqdm
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import math
from tabulate import tabulate

def connect_to_mongodb():
    try:
        client = MongoClient('mongodb://localhost:27117,localhost:27118')
        db = client['MyDatabase']
        return db
    except Exception as e:
        print(f"Connection error: {e}")
        return None

def get_product_ids():
    db = connect_to_mongodb()
    if db is not None:
        return list(db['MyCollection'].find({}, {'_id': 1}))
    return []

def generate_order_data(batch_size, product_ids):
    fake = Faker(['en_US'])
    batch_data = []
    
    order_status = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
    
    for _ in range(batch_size):
        order_date = datetime.now() - timedelta(days=random.randint(0, 365))
        products = random.sample(product_ids, random.randint(1, 5))
        
        data = {
            'orderId': str(random.randint(10000000, 99999999)),
            'customerName': fake.name(),
            'customerEmail': fake.email(),
            'shippingAddress': fake.address(),
            'orderDate': order_date,
            'status': random.choice(order_status),
            'products': [{'productId': str(p['_id']), 
                         'quantity': random.randint(1, 5)} 
                        for p in products],
            'totalAmount': round(random.uniform(50.0, 5000.0), 2)
        }
        batch_data.append(data)
    return batch_data

def import_order_batch(args):
    batch_size, mongo_uri, product_ids = args
    try:
        client = MongoClient(mongo_uri)
        collection = client['MyDatabase']['OrderCollection']
        
        batch_data = generate_order_data(batch_size, product_ids)
        collection.insert_many(batch_data)
        
        client.close()
        return len(batch_data)
    except Exception as e:
        print(f"Process error: {e}")
        return 0

def parallel_import_orders(num_records, batch_size=1000):
    num_processes = mp.cpu_count()
    num_batches = math.ceil(num_records / batch_size)
    
    product_ids = get_product_ids()
    if not product_ids:
        print("No products found in MyCollection")
        return
    
    mongo_uri = 'mongodb://localhost:27117,localhost:27118'
    args_list = [(batch_size, mongo_uri, product_ids) for _ in range(num_batches)]
    
    print(f"Using {num_processes} processes")
    
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        with tqdm(total=num_records, desc="Importing orders") as pbar:
            for result in executor.map(import_order_batch, args_list):
                pbar.update(result)

def test_order_queries():
    db = connect_to_mongodb()
    if db is None:
        return
    
    results = []
    
    # Test 1: Simple Join với sort theo price và orderDate
    start_time = time.time()
    pipeline = [
        {
            '$lookup': {
                'from': 'MyCollection',
                'localField': 'products.productId',
                'foreignField': '_id',
                'as': 'product_details'
            }
        },
        { '$unwind': '$product_details' },
        { 
            '$sort': {
                'orderDate': -1,
                'product_details.price': -1
            }
        },
        { '$limit': 1000 }
    ]
    list(db['OrderCollection'].aggregate(pipeline))
    simple_join_time = time.time() - start_time
    
    # Test 2: Filtered Join với sort theo totalAmount và category
    start_time = time.time()
    pipeline = [
        {
            '$match': {
                'status': 'Delivered',
                'totalAmount': {'$gt': 1000}
            }
        },
        {
            '$lookup': {
                'from': 'MyCollection',
                'localField': 'products.productId',
                'foreignField': '_id',
                'as': 'product_details'
            }
        },
        { '$unwind': '$product_details' },
        {
            '$sort': {
                'totalAmount': -1,
                'product_details.category': 1
            }
        },
        { '$limit': 1000 }
    ]
    list(db['OrderCollection'].aggregate(pipeline))
    filtered_join_time = time.time() - start_time
    
    # Test 3: Group By Join với sort theo total_amount
    start_time = time.time()
    pipeline = [
        {
            '$lookup': {
                'from': 'MyCollection',
                'localField': 'products.productId',
                'foreignField': '_id',
                'as': 'product_details'
            }
        },
        { '$unwind': '$product_details' },
        {
            '$group': {
                '_id': '$product_details.category',
                'total_orders': {'$sum': 1},
                'total_amount': {'$sum': '$totalAmount'},
                'avg_price': {'$avg': '$product_details.price'},
                'manufacturer_count': {'$addToSet': '$product_details.manufacturer'}
            }
        },
        {
            '$project': {
                'category': '$_id',
                'total_orders': 1,
                'total_amount': 1,
                'avg_price': 1,
                'manufacturer_count': {'$size': '$manufacturer_count'}
            }
        },
        {
            '$sort': {
                'total_amount': -1,
                'avg_price': -1
            }
        }
    ]
    list(db['OrderCollection'].aggregate(pipeline))
    group_join_time = time.time() - start_time

    # Test 4: Complex Join với nhiều điều kiện và sort
    start_time = time.time()
    pipeline = [
        {
            '$match': {
                'orderDate': {
                    '$gte': datetime.now() - timedelta(days=30)
                }
            }
        },
        {
            '$lookup': {
                'from': 'MyCollection',
                'localField': 'products.productId',
                'foreignField': '_id',
                'as': 'product_details'
            }
        },
        { '$unwind': '$product_details' },
        {
            '$match': {
                'product_details.quantity': {'$gt': 100},
                'product_details.price': {'$gt': 500}
            }
        },
        {
            '$sort': {
                'product_details.manufacturer': 1,
                'product_details.price': -1,
                'orderDate': -1
            }
        },
        { '$limit': 1000 }
    ]
    list(db['OrderCollection'].aggregate(pipeline))
    complex_join_time = time.time() - start_time
    
    results.extend([
        {
            'Query Type': 'Simple Join with Sort',
            'Execution Time (sec)': round(simple_join_time, 4)
        },
        {
            'Query Type': 'Filtered Join with Sort',
            'Execution Time (sec)': round(filtered_join_time, 4)
        },
        {
            'Query Type': 'Group By Join with Sort',
            'Execution Time (sec)': round(group_join_time, 4)
        },
        {
            'Query Type': 'Complex Join with Sort',
            'Execution Time (sec)': round(complex_join_time, 4)
        }
    ])
    
    return results

def main():
    NUM_RECORDS = 500000  # 500k orders
    BATCH_SIZE = 1000

    start_time = time.time()
    
    db = connect_to_mongodb()
    if db is not None:
        # Import data
        print("Starting order data import...")
        parallel_import_orders(NUM_RECORDS, BATCH_SIZE)
        import_time = time.time() - start_time
        print(f"Import execution time: {round(import_time, 2)} seconds")
        
        # Test queries
        print("\n=== Testing Join Queries ===")
        query_results = test_order_queries()
        print(tabulate(query_results, headers='keys', tablefmt='grid'))
        
    else:
        print("Could not connect to MongoDB")

if __name__ == "__main__":
    main()
