SELECT 
	count(1) as ecologicos
FROM car_rental_analytics
WHERE fueltype in ('hybrid', 'electric')
AND rating >=4;

SELECT 
	state_name, 
	sum(rentertripstaken) as alquileres 
FROM car_rental_analytics 
GROUP BY state_name 
ORDER BY alquileres
LIMIT 5;

SELECT 
	make, 
	model,
	sum(rentertripstaken) as alquileres 
FROM car_rental_analytics 
GROUP BY make, model 
ORDER BY alquileres DESC
LIMIT 10;

SELECT 
	year, 
	COUNT(rentertripstaken) as alquileres 
FROM car_rental_analytics 
WHERE 
	year BETWEEN 2010 AND 2015
GROUP BY year;

SELECT 
	city, 
	COUNT(rentertripstaken) as alquileres
FROM car_rental_analytics 
WHERE 
	fueltype in ('hybrid','electric') 
GROUP BY city 
ORDER BY alquileres DESC 
LIMIT 5;

SELECT 
	fueltype, 
	avg(reviewcount) as promedio_reviews
FROM car_rental_analytics
GROUP BY fueltype;