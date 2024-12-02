/*
Total de ventas y productos vendidos por tenant_id
*/
SELECT
    tenant_id,
    SUM(data.total_sales) AS total_sales,
    SUM(data.total_items) AS total_items
FROM "AwsDataCatalog"."catalogo"."api-reportes-dev"
GROUP BY tenant_id
ORDER BY total_sales DESC;

/* VISTA */

CREATE OR REPLACE VIEW "AwsDataCatalog"."catalogo"."api_sales_summary" AS
SELECT
    tenant_id,
    SUM(data.total_sales) AS total_sales,
    SUM(data.total_items) AS total_items
FROM "AwsDataCatalog"."catalogo"."api-reportes-dev"
GROUP BY tenant_id
ORDER BY total_sales DESC;

/*
Productos con bajo inventario
*/
SELECT
    product_name,
    tenant_id,
    stock_available,
    last_update
FROM "AwsDataCatalog"."catalogo"."inventoryservice-dev"
WHERE stock_available < 50
ORDER BY stock_available ASC;
/* VISTA */
CREATE OR REPLACE VIEW "AwsDataCatalog"."catalogo"."low_inventory_products" AS
SELECT
    product_name,
    tenant_id,
    stock_available,
    last_update
FROM "AwsDataCatalog"."catalogo"."inventoryservice-dev"
WHERE stock_available < 50
ORDER BY stock_available ASC;
/*
Detalles de pagos por estado
*/
SELECT
    status,
    COUNT(*) AS total_transactions,
    SUM(payment_details.amount) AS total_amount
FROM "AwsDataCatalog"."catalogo"."billingservice-dev"
GROUP BY status
ORDER BY total_amount DESC;
/* VISTA */
CREATE OR REPLACE VIEW "AwsDataCatalog"."catalogo"."billing_status_summary" AS
SELECT
    status,
    COUNT(*) AS total_transactions,
    SUM(payment_details.amount) AS total_amount
FROM "AwsDataCatalog"."catalogo"."billingservice-dev"
GROUP BY status
ORDER BY total_amount DESC;
/*
Productos más vendidos
*/
SELECT
    t.items.product_id,
    COUNT(t.items.product_id) AS total_sold,
    SUM(t.items.price) AS total_revenue
FROM "AwsDataCatalog"."catalogo"."orderservice-dev" AS o
CROSS JOIN UNNEST(o.items) AS t (items)
GROUP BY t.items.product_id
ORDER BY total_sold DESC;
/* VISTA */
CREATE OR REPLACE VIEW "AwsDataCatalog"."catalogo"."top_selling_products" AS
SELECT
    t.items.product_id,
    COUNT(t.items.product_id) AS total_sold,
    SUM(t.items.price) AS total_revenue
FROM "AwsDataCatalog"."catalogo"."orderservice-dev" AS o
CROSS JOIN UNNEST(o.items) AS t (items)
GROUP BY t.items.product_id
ORDER BY total_sold DESC;

/*
Detalles de productos y precios por tenant_id
*/
SELECT
    tenant_id,
    product_id,
    name,
    description,
    price
FROM "AwsDataCatalog"."catalogo"."productservice-dev"
ORDER BY tenant_id, price DESC;
/* VISTA */

CREATE OR REPLACE VIEW "AwsDataCatalog"."catalogo"."products_by_tenant" AS
SELECT
    tenant_id,
    product_id,
    name,
    description,
    price
FROM "AwsDataCatalog"."catalogo"."productservice-dev"
ORDER BY tenant_id, price DESC;
