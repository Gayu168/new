import pandas as pd
from cryptography.fernet import Fernet


class user_details():
# DEFINE THE DATABASE CREDENTIALS


  def encode(self):

      user = input("Enter the user:")
      password = input("Enter the password:")
      host = input("Enter the host:")
      port = input("Enter the port:")
      database = input("Enter the database:")
      #new_password = encode(password)

      key = Fernet.generate_key()
      fernet = Fernet(key)

      encMessage = fernet.encrypt(password.encode())

      print("original string: ", password)
      print("encrypted string: ", encMessage)
      return user,encMessage,host,port,database,fernet


