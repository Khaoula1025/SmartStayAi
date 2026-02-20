# JWT, password hashing functions
from passlib.context import CryptContext 

password_context=CryptContext(schemes=["bcrypt"])

def hash_password(password:str):
    return password_context.hash(password)

def verify_password(password:str,hash_password:str):
    return password_context.verify(password,hash_password)