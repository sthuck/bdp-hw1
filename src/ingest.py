import logging
from multiprocessing import Pool

from ingestion import ingest_users, ingest_business, ingest_review
from src.ingestion import ingest_review_by_user

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


def main():
    cores = 6 # your cpu cores/3 is about a good number
    # ingest_review((1, 0))
    with Pool(cores) as p:
        p.map(ingest_business, [(cores, core_id) for core_id in range(cores)])
        p.map(ingest_users, [(cores, core_id) for core_id in range(cores)])
        p.map(ingest_review, [(cores, core_id) for core_id in range(cores)])
        p.map(ingest_review_by_user, [(cores, core_id) for core_id in range(cores)])


if __name__ == '__main__':
    main()
