import duckdb
### This file does the "join" to denomalize the data and then writes it to a file
###

def pre_process():
    with duckdb.connect("foo.db") as con:
        con.execute("drop table if exists users")
        con.execute("drop table if exists reviews")
        con.execute("drop table if exists user_reviews")
        con.execute("create table users as select * from read_json_auto('../data/yelp_academic_dataset_user.json')")
        con.execute("create table reviews as select * from read_json_auto('../data/yelp_academic_dataset_review.json')")
        con.execute("create table user_reviews as select u.user_id, u.name, r.* from users u inner join reviews r on u.user_id=r.user_id")
        con.execute("copy user_reviews to '../data/user_reviews.json'")
        con.execute("drop table users")
        con.execute("drop table reviews")
        con.execute("drop table user_reviews")

if __name__ == '__main__':
    pre_process()
