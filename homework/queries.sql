\c shirley_homework

--- Get the top 3 product types that have proven most profitable
SELECT p.product_line, SUM(profit) AS total_profit
FROM star
LEFT JOIN product_line_dim p
USING(product_line_id)
GROUP BY p.product_line
LIMIT 3;

--- Get the top 3 products by most items sold
SELECT prod_dim.product_name, COUNT(*) AS counts
FROM star
LEFT JOIN  product_dim as prod_dim
USING (product_id)
GROUP BY prod_dim.product_name
ORDER BY counts DESC
LIMIT 3;

--- Get the top 3 products by items sold per country of customer for: USA, Spain, Belgium


--- Get the most profitable day of the week
SELECT d_dim.day, SUM(profit) AS total_profit
FROM star
JOIN day_of_week_dim as d_dim
USING (day_id)
GROUP BY d_dim.day
ORDER BY total_profit DESC
LIMIT 1;

--- Get the top 3 city-quarters with the highest average profit margin in their sales
SELECT quarter_id,city_id, AVG(total_sale_value - profit) AS avg_profit_margin
FROM star
GROUP BY quarter_id, city_id
ORDER BY avg_profit_margin
LIMIT 3;


-- List the employees who have sold more goods (in $ amount) than the average employee.


-- List all the orders where the sales amount in the order is in the top 10% of all order sales amounts (BONUS: Add the employee number)
SELECT employee_id, total_sale_value FROM star
ORDER BY total_sale_value DESC
LIMIT (
    SELECT count(*) / 10 FROM star
);