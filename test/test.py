from pymongo import MongoClient
import time
from statistics import mean
from tabulate import tabulate

def connect_to_mongodb():
    client = MongoClient('mongodb://localhost:27117,localhost:27118')
    return client['MyDatabase']['MyCollection']

def measure_query_time(collection, page_size, skip=0):
    start_time = time.time()
    results = list(collection.find().skip(skip).limit(page_size))
    end_time = time.time()
    return end_time - start_time, len(results)

def test_pagination_performance():
    collection = connect_to_mongodb()
    page_sizes = [100, 500, 1000, 5000, 10000]
    iterations = 5
    results = []

    for page_size in page_sizes:
        times = []
        for i in range(iterations):
            # Test with different skip positions
            skip_positions = [0, 10000, 50000, 100000]
            for skip in skip_positions:
                execution_time, count = measure_query_time(collection, page_size, skip)
                times.append({
                    'page_size': page_size,
                    'skip': skip,
                    'execution_time': execution_time,
                    'records_returned': count
                })
        
        avg_time = mean([t['execution_time'] for t in times])
        results.append({
            'Page Size': page_size,
            'Avg Time (sec)': round(avg_time, 4),
            'Min Time (sec)': round(min([t['execution_time'] for t in times]), 4),
            'Max Time (sec)': round(max([t['execution_time'] for t in times]), 4)
        })

    return results

def test_indexed_vs_nonindexed():
    collection = connect_to_mongodb()
    page_size = 1000
    iterations = 5
    
    # Ensure index exists for price field
    collection.create_index([('price', 1)])
    
    indexed_times = []
    nonindexed_times = []
    
    for _ in range(iterations):
        # Test query using index
        start_time = time.time()
        list(collection.find({'price': {'$gt': 500}}).limit(page_size))
        indexed_times.append(time.time() - start_time)
        
        # Test query without using index
        start_time = time.time()
        list(collection.find({'description': {'$regex': 'test'}}).limit(page_size))
        nonindexed_times.append(time.time() - start_time)
    
    return {
        'Indexed Query Avg Time': round(mean(indexed_times), 4),
        'Non-indexed Query Avg Time': round(mean(nonindexed_times), 4)
    }

def main():
    print("\n=== Testing Pagination Performance ===")
    pagination_results = test_pagination_performance()
    print(tabulate(pagination_results, headers='keys', tablefmt='grid'))
    
    print("\n=== Testing Indexed vs Non-indexed Queries ===")
    index_results = test_indexed_vs_nonindexed()
    for key, value in index_results.items():
        print(f"{key}: {value} seconds")

if __name__ == "__main__":
    main()