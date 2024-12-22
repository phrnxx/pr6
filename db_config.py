db_config = {
    'host': 'localhost',
    'port': '5432',
    'dbname': 'your_database',
    'user': 'your_username',
    'password': 'your_password'
}

def get_database_url():
    return f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"