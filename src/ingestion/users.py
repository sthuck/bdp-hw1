from logging import getLogger
import logging
from cassandra.cluster import Cluster, Session, BatchStatement, ConsistencyLevel, ResponseFuture
import json
import datetime
from .base import Ingestor


def ingest_users(core_info: tuple[int, int]):
    ingestor = (Ingestor('users')
                .with_insert_statement('users', '../Queries/insert_user.cql')
                .with_transform_fn('users', transform_user)
                .with_insert_statement('users_by_date', '../Queries/insert_users_by_date.cql')
                .with_transform_fn('users_by_date', transform_user_to_user_by_date)
                .with_json_path('../data/yelp_academic_dataset_user.json'))

    ingestor.ingest(core_info)


def transform_user(user, session: Session):
    user_id = user['user_id']
    name = user['name']
    average_stars = user['average_stars']
    compliments = user.get('compliments', {})
    compliments_received = user.get('compliments_received', {})
    review_count = user['review_count']
    # TODO: parse date
    yelping_since = datetime.datetime.now() or user['yelping_since']

    return (
        user_id,
        name,
        average_stars,
        compliments,
        compliments_received,
        review_count,
        yelping_since
    )

def transform_user_to_user_by_date(user, session: Session):
    user_id = user['user_id']
    name = user['name']
    average_stars = user['average_stars']
    yelping_since = datetime.datetime.fromisoformat(user['yelping_since'])
    yelping_since_month = yelping_since.month
    yelping_since_year = yelping_since.year
    # (user_id, name, yelping_since_year, average_stars, yelping_since_month)
    return (
        user_id,
        name,
        yelping_since_year,
        average_stars,
        yelping_since_month
    )