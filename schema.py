
from sqlalchemy import (Table, Column as col,
                        Integer, String, ForeignKey)


tables = {
    'user': [
        col('id', Integer, primary_key=True),
        col('first_name', String),
        col('last_name', String),
        col('fullname', String)
    ],
     'address': [
        col('id', Integer, primary_key=True),
        col('street', String),
        col('number', String),
        col('city', String),
        col('user_id', Integer, ForeignKey('user.id'))
    ],
    'post': {
        col('id', Integer, primary_key=True),
        col('title', String),
        col('content', String)
    },
    'category': {
        col('id', Integer, primary_key=True),
        col('label', String)
    },
    'post_category': {
        col('id', Integer, primary_key=True),
        col('post_id', Integer, ForeignKey('post.id')),
        col('category_id', Integer, ForeignKey('category.id'))
    },
    'user_post': {
        col('id', Integer, primary_key=True),
        col('post_id', Integer, ForeignKey('post.id')),
        col('user_id', Integer, ForeignKey('user.id'))
    }
}


def create_tables(connector):

    for n in tables.keys():
        Table(n, connector.metadata, *tables[n])

    connector.metadata.create_all(connector.engine)


