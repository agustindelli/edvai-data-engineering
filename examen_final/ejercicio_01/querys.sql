SELECT 
	COUNT(1) AS vuelos
FROM aeropuerto_tabla
WHERE
	tipo_de_movimiento = 'Despegue'
	AND fecha BETWEEN '2021-12-01' AND '2022-01-31';

SELECT 
	SUM(pasajeros) AS pasajeros
FROM aeropuerto_tabla
WHERE 
	tipo_de_movimiento = 'Despegue'
	AND fecha BETWEEN '2021-01-01' AND '2022-06-30'
	AND aerolinea_nombre = 'AEROLINEAS ARGENTINAS SA';
	
SELECT 
	a.fecha, 
	a.horautc, 
	a.aeropuerto AS aeropuerto_salida,
   	adts.ref AS ciudad_salida, 
	a.origen_destino AS aeropuerto_arribo,
   	adta.ref AS ciudad_arribo, 
   	a.pasajeros
FROM aeropuerto_tabla a
INNER JOIN aeropuerto_detalles_tabla adts ON adts.aeropuerto = a.aeropuerto
INNER JOIN aeropuerto_detalles_tabla adta ON adta.aeropuerto = a.origen_destino
WHERE 
	a.tipo_de_movimiento = 'Despegue'
	AND a.fecha BETWEEN '2022-01-01' AND '2022-06-30'	 
ORDER BY a.fecha DESC, a.horautc DESC;

SELECT 
	aerolinea_nombre, 
	SUM(pasajeros) AS pasajeros
FROM aeropuerto_tabla
WHERE 
	tipo_de_movimiento = 'Despegue'
	AND fecha BETWEEN '2021-01-01' AND '2022-06-30'
	AND aerolinea_nombre != '0'
GROUP BY aerolinea_nombre
ORDER BY pasajeros DESC
LIMIT 10;

SELECT 
	a.aeronave, 
	COUNT(1) AS usos
FROM aeropuerto_tabla a
INNER JOIN aeropuerto_detalles_tabla ad ON ad.aeropuerto = a.aeropuerto
WHERE
	a.tipo_de_movimiento = 'Despegue'
	AND a.fecha BETWEEN '2021-01-01' AND '2022-06-30'
	AND a.aeronave != '0'
	AND ad.provincia LIKE '%BUENOS AIRES%'
GROUP BY a.aeronave
ORDER BY usos DESC
LIMIT 10;