package ru.bigdata.trino.jobs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import ru.bigdata.trino.service.ClickHouseTableInitializer;

@Service
public class EtlJobGetData {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ClickHouseTableInitializer tableInitializer;

    public void getAllData() {
        tableInitializer.createSchema();
        tableInitializer.createTables();
        System.out.println("Schema and tables created");
        executeCompleteEtlProcess();
    }

    private void executeCompleteEtlProcess() {
        insertPetTypes();
        insertCountries();
        insertStates();
        insertCities();

        insertPetBreeds();
        insertProductCategories();
        insertPetCategories();
        insertColors();
        insertBrands();
        insertMaterials();

        insertSuppliers();
        insertStores();
        insertSellers();
        insertPets();
        insertCustomers();
        insertProducts();

        insertFactSales();
    }

    private void insertPetTypes() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.dim_pet_types (pet_type_id, name)
            SELECT 
                row_number() OVER () as pet_type_id,
                customer_pet_type as name
            FROM (
                SELECT DISTINCT customer_pet_type 
                FROM (
                    SELECT customer_pet_type FROM postgres.public.sales WHERE customer_pet_type IS NOT NULL
                    UNION ALL
                    SELECT customer_pet_type FROM clickhouse.default.sales WHERE customer_pet_type IS NOT NULL
                ) AS combined_sales
                WHERE customer_pet_type IS NOT NULL
            ) t
        """);
    }

    private void insertCountries() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.dim_countries (country_id, name)
            SELECT 
                row_number() OVER () as country_id,
                name
            FROM (
                SELECT DISTINCT name 
                FROM (
                    (SELECT DISTINCT customer_country AS name FROM postgres.public.sales WHERE customer_country IS NOT NULL)
                    UNION ALL
                    (SELECT DISTINCT seller_country AS name FROM postgres.public.sales WHERE seller_country IS NOT NULL)
                    UNION ALL
                    (SELECT DISTINCT store_country AS name FROM postgres.public.sales WHERE store_country IS NOT NULL)
                    UNION ALL
                    (SELECT DISTINCT supplier_country AS name FROM postgres.public.sales WHERE supplier_country IS NOT NULL)
                    UNION ALL
                    (SELECT DISTINCT customer_country AS name FROM clickhouse.default.sales WHERE customer_country IS NOT NULL)
                    UNION ALL
                    (SELECT DISTINCT seller_country AS name FROM clickhouse.default.sales WHERE seller_country IS NOT NULL)
                    UNION ALL
                    (SELECT DISTINCT store_country AS name FROM clickhouse.default.sales WHERE store_country IS NOT NULL)
                    UNION ALL
                    (SELECT DISTINCT supplier_country AS name FROM clickhouse.default.sales WHERE supplier_country IS NOT NULL)
                ) as tbl
            ) t
        """);
    }

    private void insertStates() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.dim_states (state_id, name)
            SELECT 
                row_number() OVER () as state_id,
                store_state as name
            FROM (
                SELECT DISTINCT store_state 
                FROM (
                    SELECT store_state FROM postgres.public.sales WHERE store_state IS NOT NULL
                    UNION ALL
                    SELECT store_state FROM clickhouse.default.sales WHERE store_state IS NOT NULL
                ) AS combined_sales
            ) t
        """);
    }

    private void insertCities() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.dim_cities (city_id, name)
            SELECT 
                row_number() OVER () as city_id,
                name
            FROM (
                SELECT DISTINCT name 
                FROM (
                    (SELECT DISTINCT store_city AS name FROM postgres.public.sales WHERE store_city IS NOT NULL)
                    UNION ALL
                    (SELECT DISTINCT supplier_city AS name FROM postgres.public.sales WHERE supplier_city IS NOT NULL)
                    UNION ALL
                    (SELECT DISTINCT store_city AS name FROM clickhouse.default.sales WHERE store_city IS NOT NULL)
                    UNION ALL
                    (SELECT DISTINCT supplier_city AS name FROM clickhouse.default.sales WHERE supplier_city IS NOT NULL)
                ) AS tbl
            ) t
        """);
    }

    private void insertPetBreeds() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.dim_pet_breeds (pet_breed_id, pet_type_id, name)
            SELECT 
                row_number() OVER () as pet_breed_id,
                pt.pet_type_id,
                s.customer_pet_breed as name
            FROM (
                SELECT DISTINCT s.customer_pet_type, s.customer_pet_breed
                FROM (
                    SELECT customer_pet_type, customer_pet_breed FROM postgres.public.sales
                    UNION ALL
                    SELECT customer_pet_type, customer_pet_breed FROM clickhouse.default.sales
                ) AS s
                WHERE s.customer_pet_breed IS NOT NULL
            ) s
            LEFT JOIN clickhouse.snowflake.dim_pet_types pt ON pt.name = s.customer_pet_type
        """);
    }

    private void insertProductCategories() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.dim_product_categories (category_id, name)
            SELECT 
                row_number() OVER () as category_id,
                product_category as name
            FROM (
                SELECT DISTINCT product_category 
                FROM (
                    SELECT product_category FROM postgres.public.sales WHERE product_category IS NOT NULL
                    UNION ALL
                    SELECT product_category FROM clickhouse.default.sales WHERE product_category IS NOT NULL
                ) AS combined_sales
            ) t
        """);
    }

    private void insertPetCategories() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.dim_pet_categories (pet_category_id, name)
            SELECT 
                row_number() OVER () as pet_category_id,
                pet_category as name
            FROM (
                SELECT DISTINCT pet_category 
                FROM (
                    SELECT pet_category FROM postgres.public.sales WHERE pet_category IS NOT NULL
                    UNION ALL
                    SELECT pet_category FROM clickhouse.default.sales WHERE pet_category IS NOT NULL
                ) AS combined_sales
            ) t
        """);
    }

    private void insertColors() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.dim_colors (color_id, name)
            SELECT 
                row_number() OVER () as color_id,
                product_color as name
            FROM (
                SELECT DISTINCT product_color
                FROM (
                    SELECT product_color FROM postgres.public.sales WHERE product_color IS NOT NULL
                    UNION ALL
                    SELECT product_color FROM clickhouse.default.sales WHERE product_color IS NOT NULL
                ) AS combined_sales
            ) t
        """);
    }

    private void insertBrands() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.dim_brands (brand_id, name)
            SELECT 
                row_number() OVER () as brand_id,
                product_brand as name
            FROM (
                SELECT DISTINCT product_brand
                FROM (
                    SELECT product_brand FROM postgres.public.sales WHERE product_brand IS NOT NULL
                    UNION ALL
                    SELECT product_brand FROM clickhouse.default.sales WHERE product_brand IS NOT NULL
                ) AS combined_sales
            ) t
        """);
    }

    private void insertMaterials() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.dim_materials (material_id, name)
            SELECT 
                row_number() OVER () as material_id,
                product_material as name
            FROM (
                SELECT DISTINCT product_material
                FROM (
                    SELECT product_material FROM postgres.public.sales WHERE product_material IS NOT NULL
                    UNION ALL
                    SELECT product_material FROM clickhouse.default.sales WHERE product_material IS NOT NULL
                ) AS combined_sales
            ) t
        """);
    }

    private void insertSuppliers() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.dim_suppliers (supplier_id, name, contact, email, phone, address, city_id, country_id)
            SELECT 
                row_number() OVER () as supplier_id,
                s.supplier_name,
                s.supplier_contact,
                s.supplier_email,
                s.supplier_phone,
                s.supplier_address,
                ct.city_id,
                cntr.country_id
            FROM (
                SELECT DISTINCT
                    supplier_name,
                    supplier_contact,
                    supplier_email,
                    supplier_phone,
                    supplier_address,
                    supplier_city,
                    supplier_country
                FROM (
                    SELECT 
                        supplier_name,
                        supplier_contact,
                        supplier_email,
                        supplier_phone,
                        supplier_address,
                        supplier_city,
                        supplier_country
                    FROM postgres.public.sales
                    UNION ALL
                    SELECT 
                        supplier_name,
                        supplier_contact,
                        supplier_email,
                        supplier_phone,
                        supplier_address,
                        supplier_city,
                        supplier_country
                    FROM clickhouse.default.sales
                ) s
                WHERE supplier_name IS NOT NULL
            ) s
            LEFT JOIN clickhouse.snowflake.dim_cities ct ON ct.name = s.supplier_city
            LEFT JOIN clickhouse.snowflake.dim_countries cntr ON cntr.name = s.supplier_country
        """);
    }

    private void insertStores() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.dim_stores (store_id, name, location, country_id, state_id, city_id, phone, email)
            SELECT 
                row_number() OVER () as store_id,
                s.store_name,
                s.store_location,
                cntr.country_id,
                st.state_id,
                c.city_id,
                s.store_phone,
                s.store_email
            FROM (
                SELECT DISTINCT
                    store_name,
                    store_location,
                    store_country,
                    store_state,
                    store_city,
                    store_phone,
                    store_email
                FROM (
                    SELECT 
                        store_name,
                        store_location,
                        store_country,
                        store_state,
                        store_city,
                        store_phone,
                        store_email
                    FROM postgres.public.sales
                    UNION ALL
                    SELECT 
                        store_name,
                        store_location,
                        store_country,
                        store_state,
                        store_city,
                        store_phone,
                        store_email
                    FROM clickhouse.default.sales
                ) s
                WHERE store_name IS NOT NULL
            ) s
            LEFT JOIN clickhouse.snowflake.dim_countries cntr ON cntr.name = s.store_country
            LEFT JOIN clickhouse.snowflake.dim_states st ON st.name = s.store_state
            LEFT JOIN clickhouse.snowflake.dim_cities c ON c.name = s.store_city
        """);
    }

    private void insertSellers() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.dim_sellers (seller_id, first_name, last_name, email, country_id, postal_code)
            SELECT 
                row_number() OVER () as seller_id,
                s.seller_first_name,
                s.seller_last_name,
                s.seller_email,
                cntr.country_id,
                s.seller_postal_code
            FROM (
                SELECT DISTINCT 
                    sale_seller_id,
                    seller_first_name,
                    seller_last_name,
                    seller_email,
                    seller_country,
                    seller_postal_code
                FROM (
                    SELECT 
                        sale_seller_id,
                        seller_first_name,
                        seller_last_name,
                        seller_email,
                        seller_country,
                        seller_postal_code
                    FROM postgres.public.sales
                    UNION ALL
                    SELECT 
                        sale_seller_id,
                        seller_first_name,
                        seller_last_name,
                        seller_email,
                        seller_country,
                        seller_postal_code
                    FROM clickhouse.default.sales
                ) s
                WHERE sale_seller_id IS NOT NULL
            ) s
            LEFT JOIN clickhouse.snowflake.dim_countries cntr ON cntr.name = s.seller_country
        """);
    }

    private void insertPets() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.dim_pets (pet_id, breed_id, name)
            SELECT 
                row_number() OVER () as pet_id,
                pb.pet_breed_id,
                s.customer_pet_name
            FROM (
                SELECT DISTINCT 
                    sale_customer_id,
                    customer_pet_type,
                    customer_pet_breed,
                    customer_pet_name
                FROM (
                    SELECT 
                        sale_customer_id,
                        customer_pet_type,
                        customer_pet_breed,
                        customer_pet_name
                    FROM postgres.public.sales
                    UNION ALL
                    SELECT 
                        sale_customer_id,
                        customer_pet_type,
                        customer_pet_breed,
                        customer_pet_name
                    FROM clickhouse.default.sales
                ) s
                WHERE customer_pet_name IS NOT NULL AND sale_customer_id IS NOT NULL
            ) s
            LEFT JOIN clickhouse.snowflake.dim_pet_types pt ON pt.name = s.customer_pet_type
            LEFT JOIN clickhouse.snowflake.dim_pet_breeds pb 
                ON pb.name = s.customer_pet_breed AND pb.pet_type_id = pt.pet_type_id
        """);
    }

    private void insertCustomers() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.dim_customers (customer_id, first_name, last_name, email, age, country_id, postal_code, pet_id)
            SELECT 
                row_number() OVER () as customer_id,
                c.customer_first_name,
                c.customer_last_name,
                c.customer_email,
                c.customer_age,
                cntr.country_id,
                c.customer_postal_code,
                p.pet_id
            FROM (
                SELECT DISTINCT 
                    sale_customer_id,
                    customer_first_name,
                    customer_last_name,
                    customer_email,
                    customer_age,
                    customer_country,
                    customer_postal_code,
                    customer_pet_name
                FROM (
                    SELECT 
                        sale_customer_id,
                        customer_first_name,
                        customer_last_name,
                        customer_email,
                        customer_age,
                        customer_country,
                        customer_postal_code,
                        customer_pet_name
                    FROM postgres.public.sales
                    UNION ALL
                    SELECT 
                        sale_customer_id,
                        customer_first_name,
                        customer_last_name,
                        customer_email,
                        customer_age,
                        customer_country,
                        customer_postal_code,
                        customer_pet_name
                    FROM clickhouse.default.sales
                ) c
                WHERE sale_customer_id IS NOT NULL
            ) c
            LEFT JOIN clickhouse.snowflake.dim_countries cntr ON cntr.name = c.customer_country
            LEFT JOIN clickhouse.snowflake.dim_pets p ON p.name = c.customer_pet_name
        """);
    }

    private void insertProducts() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.dim_products (
                product_id, name, pet_category_id, category_id, price, weight, 
                color_id, size, brand_id, material_id, description, 
                rating, reviews, release_date, expiry_date, supplier_id
            )
            WITH prepared_data AS (
                SELECT 
                    s.sale_product_id as original_product_id,
                    s.product_name,
                    s.product_price,
                    s.product_weight,
                    s.product_size,
                    s.product_description,
                    s.product_rating,
                    s.product_reviews,
                    s.product_release_date,
                    s.product_expiry_date,
                    s.supplier_email,
                    s.pet_category,
                    s.product_category,
                    s.product_color,
                    s.product_material,
                    s.product_brand
                FROM (
                    SELECT 
                        sale_product_id,
                        product_name,
                        product_price,
                        product_weight,
                        product_size,
                        product_description,
                        product_rating,
                        product_reviews,
                        product_release_date,
                        product_expiry_date,
                        supplier_email,
                        pet_category,
                        product_category,
                        product_color,
                        product_material,
                        product_brand
                    FROM postgres.public.sales
                    UNION ALL
                    SELECT 
                        sale_product_id,
                        product_name,
                        product_price,
                        product_weight,
                        product_size,
                        product_description,
                        product_rating,
                        product_reviews,
                        product_release_date,
                        product_expiry_date,
                        supplier_email,
                        pet_category,
                        product_category,
                        product_color,
                        product_material,
                        product_brand
                    FROM clickhouse.default.sales
                ) s
                WHERE s.sale_product_id IS NOT NULL
            )
            SELECT 
                row_number() OVER () as product_id,
                pd.product_name,
                pc.pet_category_id,
                cat.category_id,
                pd.product_price,
                pd.product_weight,
                col.color_id,
                pd.product_size,
                b.brand_id,
                mat.material_id,
                pd.product_description,
                pd.product_rating,
                pd.product_reviews,
                CASE 
                    WHEN regexp_like(pd.product_release_date, '^\\d{1,2}/\\d{1,2}/\\d{4}$') 
                    THEN date_parse(pd.product_release_date, '%m/%d/%Y') 
                    ELSE NULL 
                END,
                CASE 
                    WHEN regexp_like(pd.product_expiry_date, '^\\d{1,2}/\\d{1,2}/\\d{4}$') 
                    THEN date_parse(pd.product_expiry_date, '%m/%d/%Y') 
                    ELSE NULL 
                END,
                sup.supplier_id
            FROM prepared_data pd
            LEFT JOIN clickhouse.snowflake.dim_pet_categories pc ON pc.name = pd.pet_category
            LEFT JOIN clickhouse.snowflake.dim_product_categories cat ON cat.name = pd.product_category
            LEFT JOIN clickhouse.snowflake.dim_colors col ON col.name = pd.product_color
            LEFT JOIN clickhouse.snowflake.dim_brands b ON b.name = pd.product_brand
            LEFT JOIN clickhouse.snowflake.dim_materials mat ON mat.name = pd.product_material
            LEFT JOIN clickhouse.snowflake.dim_suppliers sup ON sup.email = pd.supplier_email
        """);
    }

    private void insertFactSales() {
        jdbcTemplate.update("""
            INSERT INTO clickhouse.snowflake.fact_sales (sale_id, customer_id, seller_id, product_id, store_id, quantity, total_price, date)
            SELECT 
                row_number() OVER () as sale_id,
                c.customer_id,
                s.seller_id,
                p.product_id,
                st.store_id,
                fs.sale_quantity,
                fs.sale_total_price,
                CASE 
                    WHEN regexp_like(fs.sale_date, '^\\d{1,2}/\\d{1,2}/\\d{4}$') 
                    THEN date_parse(fs.sale_date, '%m/%d/%Y') 
                    ELSE NULL 
                END as date
            FROM (
                SELECT 
                    sale_customer_id,
                    sale_seller_id,
                    sale_product_id,
                    store_email,
                    sale_quantity,
                    sale_total_price,
                    sale_date
                FROM postgres.public.sales
                UNION ALL
                SELECT 
                    sale_customer_id,
                    sale_seller_id,
                    sale_product_id,
                    store_email,
                    sale_quantity,
                    sale_total_price,
                    sale_date
                FROM clickhouse.default.sales
            ) fs
            LEFT JOIN clickhouse.snowflake.dim_customers c ON c.customer_id = fs.sale_customer_id
            LEFT JOIN clickhouse.snowflake.dim_sellers s ON s.seller_id = fs.sale_seller_id
            LEFT JOIN clickhouse.snowflake.dim_products p ON p.product_id = fs.sale_product_id
            LEFT JOIN clickhouse.snowflake.dim_stores st ON st.email = fs.store_email
        """);
    }
}