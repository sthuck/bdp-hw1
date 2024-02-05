CREATE KEYSPACE "Yelp"
  WITH REPLICATION = {
    'class' : 'SimpleStrategy',
    'replication_factor' : 1
  }
AND DURABLE_WRITES = false;

USE "Yelp";

CREATE TYPE IF NOT EXISTS "Yelp".hours (
  "Monday" ascii,
  "Tuesday" ascii,
  "Friday" ascii,
  "Wednesday" ascii,
  "Thursday" ascii,
  "Sunday" ascii,
  "Saturday" ascii
);

CREATE TABLE "Yelp"."users" (
  "user_id" ascii,
  "name" text,
  "yelping_since" timestamp,
  "review_count" int,
  "friends" set<text>,
  "average_stars" float,
  "compliments" map<text, int>,
  "compliments_received" map<text, int>,
  PRIMARY KEY ("user_id", "name")
)
WITH CLUSTERING ORDER BY ("name" ASC);

CREATE TABLE "Yelp"."business" (
  "business_id" text,
  "stars" float,
  "name" text,
  "address" text,
  "city" text,
  "state" text,
  "postal_code" text,
  "latitude" float,
  "longitude" float,
  "review_count" int,
  "is_open" int,
  "categories" list<text>,
  "attributes" map<text, text>,
  "hours" hours,
  PRIMARY KEY ("business_id", "stars")
)
WITH CLUSTERING ORDER BY ("stars" DESC);

CREATE TABLE "Yelp"."reviews" (
  "review_id" text,
  "stars" float,
  "user_id" text,
  "business_id" text,
  "useful" int,
  "funny" int,
  "cool" int,
  "text" text,
  "date" timestamp,
  PRIMARY KEY ("review_id", "stars")
)
WITH CLUSTERING ORDER BY ("stars" DESC);

CREATE TABLE "Yelp"."business_by_state" (
  "state" text,
  "stars" float,
  "business_id" text,
  "name" text,
  "address" text,
  "postal_code" text,
  "latitude" float,
  "longitude" float,
  "review_count" int,
  "city" text,
  PRIMARY KEY ("state", "stars", "business_id")
)
WITH CLUSTERING ORDER BY ("stars" DESC, "business_id" ASC);

CREATE TABLE "Yelp"."business_by_state_city" (
  "state" text,
  "city" text,
  "stars" float,
  "business_id" text,
  "name" text,
  "address" text,
  "postal_code" text,
  "latitude" float,
  "longitude" float,
  "review_count" int,
  PRIMARY KEY (("state", "city"), "stars", "business_id")
)
WITH CLUSTERING ORDER BY ("stars" DESC, "business_id" ASC);

CREATE TABLE "Yelp"."reviews_by_user" (
  "user_id" text,
  "stars" float,
  "business_id" text,
  "review_id" text,
  "useful" int,
  "funny" int,
  "cool" int,
  "text" text,
  "date" timestamp,
  "user_name" text,
  PRIMARY KEY ("user_id", "stars", "business_id")
)
WITH CLUSTERING ORDER BY ("stars" DESC, "review_id" ASC);

CREATE TABLE "Yelp"."users_by_date" (
  "yelping_since_year" int,
  "yelping_since_month" int,
  "average_stars" float,
  "user_id" ascii,
  "name" text,
  PRIMARY KEY (("yelping_since_year", "yelping_since_month"), "average_stars")
)
WITH CLUSTERING ORDER BY ("average_stars" DESC);

CREATE TABLE "Yelp"."business_by_state_city_category" (
  "state" text,
  "city" text,
  "category" text,
  "stars" float,
  "business_id" text,
  "name" text,
  "review_count" int,
  PRIMARY KEY (("state", "city", "category"), "stars", "business_id")
)
WITH CLUSTERING ORDER BY ("stars" DESC, "business_id" ASC);