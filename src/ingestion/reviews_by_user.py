import datetime
import pandas as pd
from cassandra.cluster import Session

from .base import Ingestor


def ingest_review_by_user(core_info: tuple[int, int]):
    ingestor = (Ingestor('reviews_by_user')
                .with_insert_statement('reviews_by_user', '../Queries/insert_review_by_user.cql')
                .with_transform_fn('reviews_by_user', transform_review)
                .with_json_path('../data/user_reviews.json'))

    ingestor.ingest(core_info)


def transform_review(review_by_user: dict, session: Session):
    business_id = review_by_user['business_id']
    cool = review_by_user['cool']
    date = datetime.datetime.fromisoformat(review_by_user['date'])
    funny = review_by_user['funny']
    review_id = review_by_user['review_id']
    stars = review_by_user['stars']
    text = review_by_user['text']
    useful = review_by_user['useful']
    user_id = review_by_user['user_id']
    user_name = review_by_user['name']

    return (
        business_id, stars, cool, date, funny, review_id, text, useful, user_id, user_name
    )

