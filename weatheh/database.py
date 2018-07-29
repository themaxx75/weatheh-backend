import os

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

DB_NAME = "weatheh.db"
DB_FILE = os.path.join(os.path.dirname(__file__), "../{}".format(DB_NAME))

engine = create_engine("sqlite:///{}".format(DB_FILE), convert_unicode=True)
db_session = scoped_session(
    sessionmaker(
        autocommit=False, autoflush=False, expire_on_commit=False, bind=engine
    )
)

Base = declarative_base()
Base.query = db_session.query_property()


def init_db():
    # import all modules here that might define models so that
    # they will be registered properly on the metadata.  Otherwise
    # you will have to import them first before calling init_db()
    # noinspection PyUnresolvedReferences
    from weatheh import models

    Base.metadata.create_all(bind=engine)
