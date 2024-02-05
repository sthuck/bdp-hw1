
select yelping_since_year, yelping_since_month, count(1) as cnt from "Yelp".users_by_date group by yelping_since_year, yelping_since_month