from locust import between

from mongo_user import MongoUser, mongodb_task
from settings import DEFAULTS

import pymongo
import random

# number of cache entries for queries
NAMES_TO_CACHE = 1000

class MongoSampleUser(MongoUser):
    """
    Generic sample mongodb workload generator
    """
    # no delays between operations
    wait_time = between(0.0, 0.0)

    def __init__(self, environment):
        super().__init__(environment)
        self.product_cache = []
        self.category_cache = []
        self.customer_cache = []
        self.order_cache = []
    def generate_product_table(self):
        """
        Generate Product Table
        """
        product_table ={
	    'product_name':self.faker.word(),
        'category_id': self.faker.pyint(min_value=0,max_value=5000),
        'price': self.faker.pydecimal(min_value=1,max_value=5000,right_digits=2),
        'quantity_available': self.faker.pyint(min_value=0,max_value=5000),
        'discount_percentage': self.faker.pydecimal(min_value=0,max_value=1,right_digits=2),
	    'description': self.faker.paragraph(nb_sentences=3,variable_nb_sentences=True),
	    'image_url': self.faker.url(),
        'created_at':self.faker.date_time()
	    
        }
        return product_table
    
    def generate_category_table(self):
        """
        Generate Category Table
        """
        category_table ={
            'category_name': self.faker.word(),
	        'description': self.faker.paragraph(nb_sentences=3,variable_nb_sentences=True),
        }
        return category_table
    
    def generate_customer_table(self):
        """
        Generate Customer Table
        """
        customer_table ={
            'name': self.faker.name(),
            'email': self.faker.email(),
            'phone_number':self.faker.phone_number(),
            'address':self.faker.address(),
	        'city':self.faker.city(),
            'country':self.faker.country(),
	        'postal_code':self.faker.postcode(),
	        'created_at':self.faker.date_time(),
        }
        return customer_table
    
    def generate_order_table(self):
        """
        Generate Order Table
        """
        order_table ={
            'customer_id': self.faker.pyint(min_value=0,max_value=10000000),
            'order_date': self.faker.date_time(),
            'total_amount':self.faker.pydecimal(min_value=1,max_value=500000,right_digits=2),
            'payment_status':self.faker.word(),
	        'shipping_address':self.faker.address(),
	        'shipping_city':self.faker.city(),
	        'shipping_country':self.faker.country(),
            'shipping_postal_code':self.faker.postcode(),
	        'created_at':self.faker.date_time(),
        }
        return order_table

    @mongodb_task(weight=int(DEFAULTS['AGG_PIPE_WEIGHT']))
    def run_aggregation_pipeline(self):
        """
        Run an aggregation pipeline on a secondary node
        """
        # count number of products per category
        group_by = {
            '$group': {
                '_id': '$category_id',
                'total_products': {'$sum': 1}
            }
        }

        # rename the _id to city
        set_columns = {'$set': {'category_id': '$_id'}}
        unset_columns = {'$unset': ['_id']}

        # sort by the number of inhabitants desc
        order_by = {'$sort': {'total_products': pymongo.DESCENDING}}

        pipeline = [group_by, set_columns, unset_columns, order_by]

        # make sure we fetch everything by explicitly casting to list
        # use self.collection instead of self.collection_secondary to run the pipeline on the primary
        return list(self.collection_secondary.aggregate(pipeline))

    def on_start(self):
        """
        Executed every time a new test is started - place init code here
        """
        # prepare the collection
        index1 = pymongo.IndexModel([('product_name', pymongo.ASCENDING)],
                                    name="idx_first_last")
        self.collection, self.collection_secondary = self.ensure_collection(DEFAULTS['COLLECTION_NAME'], [index1])
        self.product_cache = []
        self.category_cache = []
        self.customer_cache = []
        self.order_cache = []

    @mongodb_task(weight=int(DEFAULTS['INSERT_WEIGHT']))
    def insert_product(self):
        document = self.generate_product_table()

        # cache the first_name, last_name tuple for queries
        cached_products = (document['product_name'])
        if len(self.product_cache) < NAMES_TO_CACHE:
            self.product_cache.append(cached_products)
        else:
            if random.randint(0, 9) == 0:
                self.product_cache[random.randint(0, len(self.product_cache) - 1)] = cached_products

        self.collection.insert_one(document)

    @mongodb_task(weight=int(DEFAULTS['FIND_WEIGHT']))
    def find_product(self):
        # at least one insert needs to happen
        if not self.product_cache:
            return

        # find a random document using an index
        cached_products = random.choice(self.product_cache)
        self.collection.find_one({'product_name': cached_products[0]})

    @mongodb_task(weight=int(DEFAULTS['BULK_INSERT_WEIGHT']), batch_size=10)
    def insert_products_bulk_10(self):
        self.collection.insert_many(
            [self.generate_product_table() for _ in
             range(10)])
        
    @mongodb_task(weight=int(DEFAULTS['BULK_INSERT_WEIGHT']), batch_size=100)
    def insert_products_bulk_100(self):
        self.collection.insert_many(
            [self.generate_product_table() for _ in
             range(100)])
        
    @mongodb_task(weight=int(DEFAULTS['BULK_INSERT_WEIGHT']), batch_size=1000)
    def insert_products_bulk_1000(self):
        self.collection.insert_many(
            [self.generate_product_table() for _ in
             range(1000)])
        
    @mongodb_task(weight=int(DEFAULTS['INSERT_WEIGHT']))
    def insert_category(self):
        document = self.generate_category_table()

        # cache the first_name, last_name tuple for queries
        cached_categories = (document['category_name'])
        if len(self.category_cache) < NAMES_TO_CACHE:
            self.category_cache.append(cached_categories)
        else:
            if random.randint(0, 9) == 0:
                self.category_cache[random.randint(0, len(self.category_cache) - 1)] = cached_categories

        self.collection.insert_one(document)

    @mongodb_task(weight=int(DEFAULTS['FIND_WEIGHT']))
    def find_category(self):
        # at least one insert needs to happen
        if not self.category_cache:
            return

        # find a random document using an index
        cached_categories = random.choice(self.category_cache)
        self.collection.find_one({'category_name': cached_categories[0]})

    @mongodb_task(weight=int(DEFAULTS['BULK_INSERT_WEIGHT']), batch_size=10)
    def insert_categories_bulk_10(self):
        self.collection.insert_many(
            [self.generate_category_table() for _ in
             range(10)])
        
    
    @mongodb_task(weight=int(DEFAULTS['BULK_INSERT_WEIGHT']), batch_size=100)
    def insert_categories_bulk_100(self):
        self.collection.insert_many(
            [self.generate_category_table() for _ in
             range(100)])
        
    
    @mongodb_task(weight=int(DEFAULTS['BULK_INSERT_WEIGHT']), batch_size=1000)
    def insert_categories_bulk_1000(self):
        self.collection.insert_many(
            [self.generate_category_table() for _ in
             range(1000)])
        

    @mongodb_task(weight=int(DEFAULTS['INSERT_WEIGHT']))
    def insert_customer(self):
        document = self.generate_customer_table()

        # cache the first_name, last_name tuple for queries
        cached_customers = (document['name'])
        if len(self.customer_cache) < NAMES_TO_CACHE:
            self.customer_cache.append(cached_customers)
        else:
            if random.randint(0, 9) == 0:
                self.customer_cache[random.randint(0, len(self.customer_cache) - 1)] = cached_customers

        self.collection.insert_one(document)

    @mongodb_task(weight=int(DEFAULTS['FIND_WEIGHT']))
    def find_customer(self):
        # at least one insert needs to happen
        if not self.customer_cache:
            return

        # find a random document using an index
        cached_customers = random.choice(self.customer_cache)
        self.collection.find_one({'name': cached_customers[0]})

    @mongodb_task(weight=int(DEFAULTS['BULK_INSERT_WEIGHT']), batch_size=10)
    def insert_customers_bulk_10(self):
        self.collection.insert_many(
            [self.generate_customer_table() for _ in
             range(10)])
        
    @mongodb_task(weight=int(DEFAULTS['BULK_INSERT_WEIGHT']), batch_size=100)
    def insert_customers_bulk_100(self):
        self.collection.insert_many(
            [self.generate_customer_table() for _ in
             range(100)])
        
    @mongodb_task(weight=int(DEFAULTS['BULK_INSERT_WEIGHT']), batch_size=1000)
    def insert_customers_bulk_1000(self):
        self.collection.insert_many(
            [self.generate_customer_table() for _ in
             range(1000)])
        
    @mongodb_task(weight=int(DEFAULTS['INSERT_WEIGHT']))
    def insert_order(self):
        document = self.generate_order_table()

        # cache the first_name, last_name tuple for queries
        cached_orders = (document['customer_id'],document['order_date'])
        if len(self.order_cache) < NAMES_TO_CACHE:
            self.order_cache.append(cached_orders)
        else:
            if random.randint(0, 9) == 0:
                self.oorder_cache[random.randint(0, len(self.order_cache) - 1)] = cached_orders

        self.collection.insert_one(document)

    @mongodb_task(weight=int(DEFAULTS['FIND_WEIGHT']))
    def find_order(self):
        # at least one insert needs to happen
        if not self.order_cache:
            return

        # find a random document using an index
        cached_orders = random.choice(self.order_cache)
        self.collection.find_one({'customer_id': cached_orders[0],'order_date':cached_orders[1]})

    @mongodb_task(weight=int(DEFAULTS['BULK_INSERT_WEIGHT']), batch_size=10)
    def insert_orders_bulk_10(self):
        self.collection.insert_many(
            [self.generate_order_table() for _ in
             range(10)])
        
    @mongodb_task(weight=int(DEFAULTS['BULK_INSERT_WEIGHT']), batch_size=100)
    def insert_orders_bulk_100(self):
        self.collection.insert_many(
            [self.generate_order_table() for _ in
             range(100)])
        
    @mongodb_task(weight=int(DEFAULTS['BULK_INSERT_WEIGHT']), batch_size=1000)
    def insert_orders_bulk_1000(self):
        self.collection.insert_many(
            [self.generate_order_table() for _ in
             range(1000)])
