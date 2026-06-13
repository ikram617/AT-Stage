from sqlalchemy import Column, Integer, String
from DataBase import Base

class Utilisateur(Base):
    __tablename__ = "utilisateurs"

    id = Column(Integer, primary_key=True, index=True)
    nom = Column(String)
    prenom = Column(String)
    username = Column(String, unique=True, index=True)
    password = Column(String) # Hashed with bcrypt
