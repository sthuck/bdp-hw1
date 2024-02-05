--This query can help us to find the average rating of a user across all his reviews
--Yes, this value was pre-aggregated in the data we received, but this show how easy it is to re-compute in casandra
select user_id, user_name, avg(stars) from "Yelp".reviews_by_user where user_id = ? group by user_id