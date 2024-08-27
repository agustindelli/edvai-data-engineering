-- 1. Obtener una lista de todas las categorías distintas.
select distinct category_name 
from categories;

-- 2. Obtener una lista de todas las regiones distintas para los clientes.
select distinct region 
from customers;

-- 3. Obtener una lista de todos los títulos de contacto distintos.
select distinct contact_title 
from customers;

-- 4. Obtener una lista de todos los clientes, ordenados por país.
select * 
from customers 
order by country;

-- 5. Obtener una lista de todos los pedidos, ordenados por id del empleado y fecha del pedido.
select * 
from orders 
order by employee_id, order_date;

-- 6. Insertar un nuevo cliente en la tabla Customers.
insert into customers (
    customer_id, company_name, contact_name, contact_title, address, city, 
    region, postal_code, country, phone, fax
) values (
    '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.'
);

-- 7. Insertar una nueva región en la tabla Región.
insert into region (
    region_id, region_description
) values (
    5, 'Northeastern'
);

-- 8. Obtener todos los clientes de la tabla Customers donde el campo Región es NULL.
select * 
from customers 
where region is null;

-- 9. Obtener Product_Name y Unit_Price de la tabla Products, y si Unit_Price es NULL, use el precio estándar de $10 en su lugar.
select 
    product_name, 
    coalesce(unit_price, 10) as unit_price
from products;

-- 10. Obtener el nombre de la empresa, el nombre del contacto y la fecha del pedido de todos los pedidos.
select 
    c.company_name, 
    c.contact_name, 
    o.order_date
from orders o
inner join customers c on c.customer_id = o.customer_id;

-- 11. Obtener la identificación del pedido, el nombre del producto y el descuento de todos los detalles del pedido y productos.
select 
    od.order_id, 
    p.product_name, 
    od.discount
from order_details od
inner join products p on p.product_id = od.product_id;

-- 12. Obtener el identificador del cliente, el nombre de la compañía, el identificador y la fecha de la orden de todas las órdenes y aquellos clientes que hagan match.
select 
    c.customer_id, 
    c.company_name, 
    o.order_id, 
    o.order_date
from orders o
left join customers c on c.customer_id = o.customer_id;

-- 13. Obtener el identificador del empleado, apellido, identificador de territorio y descripción del territorio de todos los empleados y aquellos que hagan match en territorios.
select 
    e.employee_id, 
    e.last_name, 
    et.territory_id, 
    t.territory_description
from employees e
left join employee_territories et on et.employee_id = e.employee_id
left join territories t on t.territory_id = et.territory_id;

-- 14. Obtener el identificador del pedido y el nombre de la compañía de todas las órdenes y aquellos clientes que hagan match.
select 
    o.order_id, 
    c.company_name
from orders o
left join customers c on c.customer_id = o.customer_id;

-- 15. Obtener el identificador de la orden, y el nombre de la compañía de todas las órdenes y aquellos clientes que hagan match.
select 
    o.order_id, 
    c.company_name
from customers c
right join orders o on o.customer_id = c.customer_id;

-- 16. Obtener el nombre de la compañía y la fecha de la orden de todas las órdenes y aquellos transportistas que hagan match. Solo para aquellas órdenes del año 1996.
select 
    s.company_name, 
    o.order_date
from shippers s
right join orders o on o.ship_via = s.shipper_id
where extract(year from o.order_date) = 1996;

-- 17. Obtener nombre y apellido del empleado y el identificador de territorio, de todos los empleados y aquellos que hagan match o no de employee_territories.
select 
    e.first_name, 
    e.last_name, 
    et.territory_id
from employees e
full outer join employee_territories et on et.employee_id = e.employee_id;

-- 18. Obtener el identificador de la orden, precio unitario, cantidad y total de todas las órdenes y aquellas órdenes detalles que hagan match o no.
select 
    o.order_id, 
    od.unit_price, 
    od.quantity, 
    (od.unit_price * od.quantity) as total
from orders o
full outer join order_details od on od.order_id = o.order_id;

-- 19. Obtener la lista de todos los nombres de los clientes y los nombres de los proveedores.
select company_name as nombre 
from customers
union
select company_name 
from suppliers;

-- 20. Obtener la lista de los nombres de todos los empleados y los nombres de los gerentes de departamento.
select first_name as nombre 
from employees
union
select first_name 
from employees 
where title = 'Sales Manager';

-- 21. Obtener los productos del stock que han sido vendidos.
select 
    product_name, 
    product_id
from products
where product_id in (
    select distinct product_id 
    from order_details
);

-- 22. Obtener los clientes que han realizado un pedido con destino a Argentina.
select 
    company_name
from customers
where customer_id in (
    select customer_id 
    from orders 
    where ship_country = 'Argentina'
);

-- 23. Obtener el nombre de los productos que nunca han sido pedidos por clientes de Francia.
select 
    p.product_name
from products p
where p.product_id not in (
    select 
        od.product_id 
    from customers c
    inner join orders o on o.customer_id = c.customer_id
    inner join order_details od on od.order_id = o.order_id
    where c.country = 'France'
);

-- 24. Obtener la cantidad de productos vendidos por identificador de orden.
select 
    order_id, 
    sum(quantity) as total_quantity
from order_details
group by order_id;

-- 25. Obtener el promedio de productos en stock por producto.
select 
    product_name, 
    avg(units_in_stock) as average_stock
from products
group by product_name;

-- 26. Cantidad de productos en stock por producto, donde haya más de 100 productos en stock.
select 
    product_name, 
    sum(units_in_stock) as total_stock
from products
group by product_name
having sum(units_in_stock) > 100;

-- 27. Obtener el promedio de pedidos por cada compañía y solo mostrar aquellas con un promedio de pedidos superior a 10.
select 
    c.company_name, 
    avg(o.order_id) as average_orders
from orders o
inner join customers c on c.customer_id = o.customer_id
group by c.company_name
having avg(o.order_id) > 10;

-- 28. Obtener el nombre del producto y su categoría, pero mostrar "Discontinued" en lugar del nombre de la categoría si el producto ha sido descontinuado.
select 
    p.product_name, 
    case 
        when p.discontinued = 1 then 'Discontinued' 
        else c.category_name 
    end as product_category
from products p
left join categories c on c.category_id = p.category_id;

-- 29. Obtener el nombre del empleado y su título, pero mostrar "Gerente de Ventas" en lugar del título si el empleado es un gerente de ventas (Sales Manager).
select 
    e.first_name, 
    e.last_name, 
    case 
        when e.title = 'Sales Manager' then 'Gerente de Ventas' 
        else e.title 
    end as job_title
from employees e;