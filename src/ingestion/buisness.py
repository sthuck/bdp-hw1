from logging import getLogger
import logging
from cassandra.cluster import Cluster, Session, BatchStatement, ConsistencyLevel, ResponseFuture
import json
import datetime
from .base import Ingestor

class Hours(object):
    def __init__(self, hours: dict):
        self.Monday = hours.get('Monday', None)
        self.Tuesday = hours.get('Tuesday', None)
        self.Wednesday = hours.get('Wednesday', None)
        self.Thursday = hours.get('Thursday', None)
        self.Friday = hours.get('Friday', None)
        self.Saturday = hours.get('Saturday', None)
        self.Sunday = hours.get('Sunday', None)


def ingest_business(core_info: tuple[int, int]):
    ingestor = (Ingestor('business')
                # .with_insert_statement('business', '../Queries/insert_business.cql')
                # .with_transform_fn('business', transform_business)
                # .with_insert_statement('business_by_state', '../Queries/insert_business_by_state.cql')
                # .with_transform_fn('business_by_state', transform_business_for_by_location_tables)
                # .with_insert_statement('business_by_state_city', '../Queries/insert_business_by_state_city.cql')
                # .with_transform_fn('business_by_state_city', transform_business_for_by_location_tables)
                .with_insert_statement('business_by_state_city_category', '../Queries'
                                                                          '/insert_business_by_state_city_category.cql')
                .with_transform_fn('business_by_state_city_category', transform_business_for_by_location_category_tables)
                .with_json_path('../data/yelp_academic_dataset_business.json')
                .with_udt('hours', Hours))

    ingestor.ingest(core_info)


def transform_business(business, session: Session):
    business_id = business['business_id']
    name = business['name']
    city = business['city']
    state = business['state']
    postal_code = business['postal_code']
    latitude = business['latitude']
    longitude = business['longitude']
    stars = business['stars']
    review_count = business['review_count']
    is_open = business['is_open']
    is_open = True if is_open == 1 else False
    categories = business['categories']
    attributes = business['attributes'] or {}
    hours = business['hours'] or None

    # transform Categories into a list
    categories = categories.split(', ') if categories else []
    # transform hours into a UDT
    hours = Hours(hours) if hours else None

    return (
        business_id,
        name,
        city,
        state,
        postal_code,
        latitude,
        longitude,
        stars,
        review_count,
        is_open,
        categories,
        attributes,
        hours
    )


def transform_business_for_by_location_tables(business, session: Session):
    business_id = business['business_id']
    name = business['name']
    city = business['city']
    state = business['state']
    postal_code = business['postal_code']
    latitude = business['latitude']
    longitude = business['longitude']
    stars = business['stars']
    review_count = business['review_count']

    return (
        business_id,
        name,
        city,
        state,
        postal_code,
        latitude,
        longitude,
        stars,
        review_count,
    )

def transform_business_for_by_location_category_tables(business, session: Session):
    business_id = business['business_id']
    name = business['name']
    city = business['city']
    state = business['state']
    stars = business['stars']
    review_count = business['review_count']
    is_open = business['is_open']
    categories = business['categories']
    hours = business['hours'] or None

    # transform Categories into a list
    categories = categories.split(', ') if categories else []

    return [(
        business_id,
        name,
        city,
        category,
        state,
        stars,
        review_count,
    ) for category in categories]
