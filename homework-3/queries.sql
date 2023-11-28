-- Напишите запросы, которые выводят следующую информацию:
-- 1. Название компании заказчика (company_name из табл. customers) и ФИО сотрудника, работающего над заказом этой компании (см таблицу employees),
-- когда и заказчик и сотрудник зарегистрированы в городе London, а доставку заказа ведет компания United Package (company_name в табл shippers)
SELECT customers.company_name, CONCAT(employees.first_name, ' ', employees.last_name) AS employee
FROM orders
INNER JOIN employees ON orders.employee_id=employees.employee_id
INNER JOIN customers ON orders.customer_id=customers.customer_id
INNER JOIN shippers ON orders.ship_via=shippers.shipper_id
WHERE employees.city in ('London') AND customers.city in ('London') AND shippers.company_name in ('United Package')


-- 2. Наименование продукта, количество товара (product_name и units_in_stock в табл products),
-- имя поставщика и его телефон (contact_name и phone в табл suppliers) для таких продуктов,
-- которые не сняты с продажи (поле discontinued) и которых меньше 25 и которые в категориях Dairy Products и Condiments.
-- Отсортировать результат по возрастанию количества оставшегося товара.
SELECT products.product_name, products.units_in_stock, suppliers.contact_name, suppliers.phone
FROM products
INNER JOIN suppliers USING(supplier_id)
INNER JOIN categories USING(category_id)
WHERE products.discontinued=0 AND categories.category_name in ('Dairy Products', 'Condiments') AND products.units_in_stock<25
ORDER BY products.units_in_stock

-- 3. Список компаний заказчиков (company_name из табл customers), не сделавших ни одного заказа
SELECT customers.company_name FROM orders
FULL JOIN customers USING(customer_id)
WHERE order_id is NULL
ORDER BY customers.company_name

-- 4. уникальные названия продуктов, которых заказано ровно 10 единиц (количество заказанных единиц см в колонке quantity табл order_details)
-- Этот запрос написать именно с использованием подзапроса.
SELECT product_name from products
where product_id in (select distinct product_id from order_details where quantity=10)