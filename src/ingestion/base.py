import json
import logging
from logging import getLogger
from typing import Any, Optional, Callable

from cassandra.cluster import Cluster, Session


class Ingestor:
    """Base class for all Ingestors."""

    def __init__(self, name: str):
        self.name = name
        self.insert_statements: dict[str] = {}
        self.json_path: Optional[str] = None
        self.transform_fn: dict[Callable[[dict, Session], tuple]] = {}
        self.UDT: list[tuple[str, Any]] = []

    def with_insert_statement(self, table_name: str, insert_statement: str) -> 'Ingestor':
        self.insert_statements[table_name] = insert_statement
        return self

    def with_json_path(self, json_path: str) -> 'Ingestor':
        self.json_path = json_path
        return self

    def with_udt(self, udt_name: str, udt: Any) -> 'Ingestor':
        self.UDT.append((udt_name, udt))
        return self

    def with_transform_fn(self, table_name: str, transform_fn: Callable[[dict, Session], tuple]) -> 'Ingestor':
        self.transform_fn[table_name] = transform_fn
        return self

    def ingest(self, core_info: tuple[int, int]):
        if not len(self.insert_statements):
            raise ValueError('insert_statement must be set')
        if not self.json_path:
            raise ValueError('json_path must be set')
        if not self.transform_fn:
            raise ValueError('transform_fn must be set')

        logger = getLogger(self.name)
        logger.setLevel(logging.INFO)
        logger.info(f'Starting ${self.name} ingest')

        cluster = Cluster(['127.0.0.1'])
        for udt_name, udt in self.UDT:
            cluster.register_user_type('Yelp', udt_name, udt)

        session = cluster.connect()
        session.execute('use "Yelp"')

        queries = {}
        for (table_name, insert_statement) in self.insert_statements.items():
            with open(insert_statement) as query_file:
                query = query_file.read()
                statement = session.prepare(query, keyspace='Yelp')
                queries[table_name] = statement

        total_cores, core_id = core_info

        with open(self.json_path) as f:

            total_work = 0
            index = 0
            while True:
                line = f.readline()
                if not line:
                    break

                if index % total_cores == core_id:
                    total_work += 1
                    try:
                        obj = json.loads(line)
                    except Exception as e:
                        logger.error(f'Error parsing line {line} with error {e}')
                        continue
                    for table_name, q in queries.items():
                        data = self.transform_fn[table_name](obj, session)
                        try:
                            if type(data) is list:
                                for d in data:
                                    session.execute(q, d)
                            else:
                                session.execute(q, data)
                        except Exception as e:
                            logger.error(f'Error inserting data {data} into {table_name} with error {e}')

                    if total_work % 1000 == 0:
                        logger.info(f'Core {core_id} has completed {total_work} work')
                index += 1

