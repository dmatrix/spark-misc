import faker
from faker import Faker

def generate_fake_data(num_rows=10):
    faker = Faker()
    data = []
    for _ in range(num_rows):
        data.append({
            'name': faker.name(),
            'email': faker.email(),
            'phone': faker.phone_number(),
            'address': faker.address(),
            'city': faker.city(),
            'state': faker.state(),
            'zip': faker.zipcode(),
            'country': faker.country(),
            'date': faker.date(),
            'time': faker.time(),
            'datetime': faker.date_time(),})
            
    return data


def generate_fake_data_with_schema(num_rows=10):
    """Generate fake data with a predefined schema"""
    faker = Faker()
    data = []
    
    # Define a schema for the data structure
    schema = {
        'user_id': lambda: faker.random_int(min=1000, max=9999),
        'username': lambda: faker.user_name(),
        'full_name': lambda: faker.name(),
        'email': lambda: faker.email(),
        'age': lambda: faker.random_int(min=18, max=80),
        'registration_date': lambda: faker.date_between(start_date='-2y', end_date='today'),
        'is_active': lambda: faker.boolean(),
        'balance': lambda: round(faker.random.uniform(0, 10000), 2)
    }
    
    for _ in range(num_rows):
        record = {}
        for field, generator in schema.items():
            record[field] = generator()
        data.append(record)
    return data





