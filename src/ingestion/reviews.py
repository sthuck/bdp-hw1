import datetime
import pandas as pd
from cassandra.cluster import Session

from .base import Ingestor


def ingest_review(core_info: tuple[int, int]):
    ingestor = (Ingestor('reviews')
                .with_insert_statement('review', '../Queries/insert_review.cql')
                .with_transform_fn('review', transform_review)
                # .with_insert_statement('review_by_user', '../Queries/insert_review_by_user.cql')
                # .with_transform_fn('review_by_user', transform_review_by_user)
                .with_json_path('../data/yelp_academic_dataset_review.json'))

    ingestor.ingest(core_info)

##
## This part implemented ingestion to a denormalized table, by querying a different table in cassandra
## thus making it possible to ingest data even if joined data doesn't fit in memory
## but this was too slow for a home assignment - so we "cheated" and joined the data in memory in pre-process stage

# statement = None
# user_id_map = None
#
# def transform_review_by_user(review: dict, session: Session):
#     global statement
#     global user_id_map
#     statement = statement or session.prepare('select name from users where user_id = ?')
#     user_id = review['user_id']
#     result = session.execute(statement, [review['user_id']])
#     row = result.one()
#     user_name = row.name if row else 'Unknown'
#
#     business_id = review['business_id']
#     cool = review['cool']
#     date = datetime.datetime.fromisoformat(review['date'])
#     funny = review['funny']
#     review_id = review['review_id']
#     stars = review['stars']
#     text = review['text']
#     useful = review['useful']
#
#     return (
#         business_id, stars, cool, date, funny, review_id, text, useful, user_id, user_name
#     )

def transform_review(review: dict, session: Session):
    business_id = review['business_id']
    cool = review['cool']
    date =  datetime.datetime.fromisoformat(review['date'])
    funny = review['funny']
    review_id = review['review_id']
    stars = review['stars']
    text = review['text']
    useful = review['useful']
    user_id = review['user_id']

    return (
        business_id, stars, cool, date, funny, review_id, text, useful, user_id
    )

