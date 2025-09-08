import logging

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy_utils import database_exists, create_database

Base = declarative_base()

logger = logging.getLogger(__name__)
ScopedSession = scoped_session(sessionmaker())


def init_db(db_uri):
    engine = create_engine(db_uri)
    if not database_exists(engine.url):
        logger.info('creating db')
        create_database(engine.url)
    Base.metadata.create_all(engine)
    ScopedSession.configure(bind=engine)
