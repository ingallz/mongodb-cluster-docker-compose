from pymongo import MongoClient
import time
from datetime import datetime, timedelta
from tabulate import tabulate
from typing import List, Dict
import pymongo

def connect_to_mongodb():
    try:
        client = MongoClient('mongodb://localhost:27117,localhost:27118')
        db = client['MyDatabase']
        return db
    except Exception as e:
        print(f"Connection error: {e}")
        return None

def test_simple_queries(collection, page_sizes: List[int]) -> List[Dict]:
    results = []
    
    sort_fields = [
        [('price', -1)],  # Sort theo giá
        [('createdAt', -1)],  # Sort theo thời gian tạo
        [('manufacturer', 1)],  # Sort theo nhà sản xuất
        [('category', 1), ('price', -1)],  # Sort theo danh mục và giá
        [('quantity', -1)],  # Sort theo số lượng
        [('oemNumber', 1)]  # Sort theo mã OEM
    ]
    
    for page_size in page_sizes:
        for sort_field in sort_fields:
            sort_name = '+'.join(f[0] for f in sort_field)
            
            start_time = time.time()
            cursor = collection.find({}).sort(sort_field).limit(page_size)
            data = list(cursor)
            execution_time = time.time() - start_time
            
            record_count = len(data)
            
            results.append({
                'Query Type': 'Simple Sort',
                'Sort Field': sort_name,
                'Page Size': page_size,
                'Records Returned': record_count,
                'Execution Time (sec)': round(execution_time, 4)
            })
    
    return results

def test_join_queries(db, page_sizes: List[int]) -> List[Dict]:
    results = []
    
    pipelines = [
        # Simple Join với Sort - Tối ưu bằng cách sort và paginate trước
        lambda size: [
            {'$sort': {'orderDate': -1}},
            {'$limit': size},
            {'$addFields': {
                'products': {
                    '$map': {
                        'input': '$products',
                        'as': 'product',
                        'in': {
                            'productId': {'$toObjectId': '$$product.productId'},
                            'quantity': '$$product.quantity'
                        }
                    }
                }
            }},
            {'$lookup': {
                'from': 'MyCollection',
                'localField': 'products.productId',
                'foreignField': '_id',
                'as': 'product_details'
            }},
            {'$unwind': '$product_details'},
            {'$project': {
                'orderDate': 1,
                'customerName': 1,
                'totalAmount': 1,
                'status': 1,
                'product': {
                    'name': '$product_details.productName',
                    'price': '$product_details.price',
                    'category': '$product_details.category',
                    'manufacturer': '$product_details.manufacturer'
                }
            }},
            {'$facet': {
                'data': [{'$match': {}}],
                'count': [{'$count': 'total'}]
            }}
        ],
        
        # Join với Group - Sort và paginate trước
        lambda size: [
            {'$sort': {'totalAmount': -1}},
            {'$limit': size},
            {'$addFields': {
                'products': {
                    '$map': {
                        'input': '$products',
                        'as': 'product',
                        'in': {
                            'productId': {'$toObjectId': '$$product.productId'},
                            'quantity': '$$product.quantity'
                        }
                    }
                }
            }},
            {'$lookup': {
                'from': 'MyCollection',
                'localField': 'products.productId',
                'foreignField': '_id',
                'as': 'product_details'
            }},
            {'$unwind': '$product_details'},
            {'$group': {
                '_id': '$product_details.category',
                'total_amount': {'$sum': '$totalAmount'},
                'order_count': {'$sum': 1},
                'avg_order_value': {'$avg': '$totalAmount'},
                'unique_customers': {'$addToSet': '$customerName'}
            }},
            {'$project': {
                '_id': 0,
                'category': '$_id',
                'total_amount': 1,
                'order_count': 1,
                'avg_order_value': 1,
                'unique_customer_count': {'$size': '$unique_customers'}
            }},
            {'$sort': {'total_amount': -1}},
            {'$facet': {
                'data': [{'$match': {}}],
                'count': [{'$count': 'total'}]
            }}
        ],
        
        # Complex Join - Filter và sort trước khi lookup
        lambda size: [
            {'$match': {
                'orderDate': {'$gte': datetime.now() - timedelta(days=30)},
                'totalAmount': {'$gt': 500},
                'status': 'Delivered'
            }},
            {'$sort': {'orderDate': -1}},
            {'$limit': size},
            {'$addFields': {
                'products': {
                    '$map': {
                        'input': '$products',
                        'as': 'product',
                        'in': {
                            'productId': {'$toObjectId': '$$product.productId'},
                            'quantity': '$$product.quantity'
                        }
                    }
                }
            }},
            {'$lookup': {
                'from': 'MyCollection',
                'localField': 'products.productId',
                'foreignField': '_id',
                'as': 'product_details'
            }},
            {'$unwind': '$product_details'},
            {'$match': {
                'product_details.quantity': {'$gt': 100},
                'product_details.price': {'$gt': 500}
            }},
            {'$project': {
                'orderDate': 1,
                'customerName': 1,
                'totalAmount': 1,
                'product': {
                    'name': '$product_details.productName',
                    'price': '$product_details.price',
                    'quantity': '$product_details.quantity',
                    'manufacturer': '$product_details.manufacturer'
                }
            }},
            {'$sort': {
                'product.price': -1,
                'orderDate': -1
            }},
            {'$facet': {
                'data': [{'$match': {}}],
                'count': [{'$count': 'total'}]
            }}
        ]
    ]
    
    pipeline_names = ['Optimized Simple Join', 'Optimized Group Join', 'Optimized Complex Join']
    
    for page_size in page_sizes:
        for pipeline_func, name in zip(pipelines, pipeline_names):
            start_time = time.time()
            result = list(db['OrderCollection'].aggregate(pipeline_func(page_size)))
            execution_time = time.time() - start_time
            
            record_count = result[0]['count'][0]['total'] if result[0]['count'] else 0
            
            results.append({
                'Query Type': name,
                'Page Size': page_size,
                'Records Returned': record_count,
                'Execution Time (sec)': round(execution_time, 4)
            })
    
    return results

def create_indexes(db):
    print("Creating indexes...")
    
    # Indexes cho MyCollection
    try:
        db['MyCollection'].create_indexes([
            # Index cho price và category - hỗ trợ sort và filter
            pymongo.IndexModel([("price", -1)]),
            pymongo.IndexModel([("category", 1), ("price", -1)]),
            # Index cho manufacturer - hỗ trợ sort
            pymongo.IndexModel([("manufacturer", 1)]),
            # Index cho _id vì nó được dùng trong join
            pymongo.IndexModel([("_id", 1)])
        ])
        
        # Indexes cho OrderCollection
        db['OrderCollection'].create_indexes([
            # Index cho orderDate - hỗ trợ filter date range
            pymongo.IndexModel([("orderDate", -1)]),
            # Index cho status và totalAmount - hỗ trợ filter
            pymongo.IndexModel([("status", 1), ("totalAmount", 1)]),
            # Index cho products.productId - hỗ trợ join
            pymongo.IndexModel([("products.productId", 1)]),
            # Compound index cho sort phức hợp
            pymongo.IndexModel([
                ("products.productId", 1),
                ("totalAmount", -1)
            ])
        ])
        print("Indexes created successfully")
    except Exception as e:
        print(f"Error creating indexes: {e}")

def main():
    db = connect_to_mongodb()
    if db is None:
        return
    
    # Tạo indexes trước khi test
    create_indexes(db)
    
    page_sizes = [10, 50, 100, 500, 1000]
    
    print("\n=== Testing Simple Queries ===")
    simple_results = test_simple_queries(db['MyCollection'], page_sizes)
    print(tabulate(simple_results, headers='keys', tablefmt='grid'))
    
    print("\n=== Testing Join Queries ===")
    join_results = test_join_queries(db, page_sizes)
    print(tabulate(join_results, headers='keys', tablefmt='grid'))

if __name__ == "__main__":
    main()