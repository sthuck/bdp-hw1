-- The purpose of this query is to find the average stars of the users by the year and month they joined Yelp
-- We are looking for some sort of "bias", if older users tend to give higher or lower ratings
select yelping_since_year, yelping_since_month, avg(average_stars) from "Yelp".users_by_date group by yelping_since_year, yelping_since_month