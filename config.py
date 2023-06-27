import os
from cryptography.fernet import Fernet
from dotenv import load_dotenv, find_dotenv
from credentails import user_details

def credentials_to_url():


    details = user_details()
    user, encMessage, host, port, database, fernet = details.encode()
    decMessage = fernet.decrypt(encMessage).decode()
    #print("decrypted string: ", decMessage)
    db_type =input("Enter the database type:")
    if db_type == 'mysql':
       Database_url = f"mysql+pymysql://{user}:{decMessage}@{host}:{port}/{database}"
       return Database_url
    else:
        Database_url = f"postgresql+psycopg2://{user}:{decMessage}@{host}:{port}/{database}"
        return Database_url


