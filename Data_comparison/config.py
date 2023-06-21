import os

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

Database_url = os.getenv('DATABASE_URL')


def credentials_to_url():
    user = os.getenv('User')
    password = os.getenv('password')
    host = os.getenv('host')
    port = os.getenv('port')
    database = os.getenv('database')
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
