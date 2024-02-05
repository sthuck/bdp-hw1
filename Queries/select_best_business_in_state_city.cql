-- This is the "core query" for a hypothetical 'yelp" like application. Find the best businesses in a given city and state, for a given category.
-- example:
-- select *  from business_by_state_city_category where state = 'CA' and city = 'Santa Barbara' and category = 'Restaurants' limit 5;
select *  from business_by_state_city_category where state = ? and city = ? and category = ? limit 5;
