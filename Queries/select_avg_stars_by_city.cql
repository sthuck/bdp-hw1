-- This query will help you find the best state to visit if you are looking for a good restaurant.
select state, city, avg(stars) as avg_star from business_by_state_city group by state, city