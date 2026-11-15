SELECT product.product_id,
       product.product_name,
       product.seller_id,
       product.category_id,
       coalesce(category.category_name, 'Unknown') AS category_name,
       coalesce(department.category_name, 'Unknown') AS department,
       attributes.brand,
       attributes.color,
       attributes.material,
       product.mrp,
       product.price,
       product.is_active,
       product.is_deleted
FROM silver.products AS product
LEFT JOIN silver.categories AS category ON category.category_id = product.category_id
LEFT JOIN silver.categories AS department ON department.category_id = category.parent_category_id
LEFT JOIN silver.product_attributes AS attributes ON attributes.product_id = product.product_id
