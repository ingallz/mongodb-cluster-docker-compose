from pymongo import MongoClient, WriteConcern
from pymongo.errors import WTimeoutError
import time
from tabulate import tabulate
from datetime import datetime

def connect_to_mongodb():
    try:
        client = MongoClient('mongodb://localhost:27117,localhost:27118')
        return client
    except Exception as e:
        print(f"Connection error: {e}")
        return None

def test_write_concerns():
    client = connect_to_mongodb()
    if not client:
        return
    
    db = client['MyDatabase']
    results = []
    
    test_data = {
        'oemNumber': 'TEST123',
        'productName': 'Test Product',
        'price': 100.0,
        'quantity': 10,
        'createdAt': datetime.now(),
        'category': 'Electronics'
    }
    
    write_concerns = [
        {'w': 1, 'wtimeout': 5000},
        {'w': 2, 'wtimeout': 5000},
        {'w': 'majority', 'wtimeout': 5000}
    ]
    
    for wc in write_concerns:
        collection = db.get_collection(
            'MyCollection',
            write_concern=WriteConcern(**wc)
        )
        
        start_time = time.time()
        success = True
        error_msg = None
        
        try:
            result = collection.insert_one(test_data)
            collection.delete_one({'_id': result.inserted_id})
        except WTimeoutError as e:
            success = False
            error_msg = "Write Timeout"
        except Exception as e:
            success = False
            error_msg = str(e)
        
        execution_time = time.time() - start_time
        
        results.append({
            'Write Concern': f"w: {wc['w']}, wtimeout: {wc['wtimeout']}ms",
            'Success': success,
            'Error': error_msg if error_msg else 'None',
            'Execution Time (sec)': round(execution_time, 4)
        })
    
    return results

def test_batch_writes():
    client = connect_to_mongodb()
    if not client:
        return
    
    db = client['MyDatabase']
    results = []
    
    batch_sizes = [100, 500, 1000]
    
    def generate_test_orders(size):
        return [{
            'orderId': f'ORDER{i}',
            'customerName': f'Customer {i}',
            'customerEmail': f'customer{i}@test.com',
            'orderDate': datetime.now(),
            'status': 'Pending',
            'products': [{'productId': 'TEST123', 'quantity': 1}]
        } for i in range(size)]
    
    write_concerns = [
        {'w': 1, 'wtimeout': 5000},
        {'w': 'majority', 'wtimeout': 5000}
    ]
    
    for batch_size in batch_sizes:
        test_data = generate_test_orders(batch_size)
        
        for wc in write_concerns:
            collection = db.get_collection(
                'OrderCollection',
                write_concern=WriteConcern(**wc)
            )
            
            start_time = time.time()
            success = True
            error_msg = None
            
            try:
                result = collection.insert_many(test_data)
                collection.delete_many({'orderId': {'$regex': '^ORDER'}})
            except WTimeoutError as e:
                success = False
                error_msg = "Write Timeout"
            except Exception as e:
                success = False
                error_msg = str(e)
            
            execution_time = time.time() - start_time
            
            results.append({
                'Batch Size': batch_size,
                'Write Concern': f"w: {wc['w']}, wtimeout: {wc['wtimeout']}ms",
                'Success': success,
                'Error': error_msg if error_msg else 'None',
                'Execution Time (sec)': round(execution_time, 4)
            })
    
    return results

def main():
    print("\n=== Testing Single Write Concerns ===")
    single_results = test_write_concerns()
    print(tabulate(single_results, headers='keys', tablefmt='grid'))
    
    print("\n=== Testing Batch Write Concerns ===")
    batch_results = test_batch_writes()
    print(tabulate(batch_results, headers='keys', tablefmt='grid'))

if __name__ == "__main__":
    main()