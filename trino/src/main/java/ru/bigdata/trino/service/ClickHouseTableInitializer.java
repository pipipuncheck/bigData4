package ru.bigdata.trino.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class ClickHouseTableInitializer {

    private final JdbcTemplate jdbcTemplate;

    public ClickHouseTableInitializer(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void createSchema() {
        jdbcTemplate.execute("CREATE SCHEMA IF NOT EXISTS clickhouse.snowflake");
    }

    public void createTables() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.dim_pet_types (
                pet_type_id INTEGER,
                name VARCHAR
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.dim_pet_breeds (
                pet_breed_id INTEGER,
                pet_type_id INTEGER,
                name VARCHAR
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.dim_pets (
                pet_id INTEGER,
                name VARCHAR,
                breed_id INTEGER
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.dim_countries (
                country_id INTEGER,
                name VARCHAR
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.dim_states (
                state_id INTEGER,
                name VARCHAR
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.dim_cities (
                city_id INTEGER,
                name VARCHAR
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.dim_customers (
                customer_id INTEGER,
                first_name VARCHAR,
                last_name VARCHAR,
                email VARCHAR,
                age INTEGER,
                country_id INTEGER,
                postal_code VARCHAR,
                pet_id INTEGER
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.dim_sellers (
                seller_id INTEGER,
                first_name VARCHAR,
                last_name VARCHAR,
                email VARCHAR,
                country_id INTEGER,
                postal_code VARCHAR
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.dim_stores (
                store_id INTEGER,
                name VARCHAR,
                location VARCHAR,
                country_id INTEGER,
                state_id INTEGER,
                city_id INTEGER,
                phone VARCHAR,
                email VARCHAR
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.dim_product_categories (
                category_id INTEGER,
                name VARCHAR
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.dim_pet_categories (
                pet_category_id INTEGER,
                name VARCHAR
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.dim_suppliers (
                supplier_id INTEGER,
                name VARCHAR,
                contact VARCHAR,
                email VARCHAR,
                phone VARCHAR,
                address VARCHAR,
                city_id INTEGER,
                country_id INTEGER
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.dim_colors (
                color_id INTEGER,
                name VARCHAR
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.dim_brands (
                brand_id INTEGER,
                name VARCHAR
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.dim_materials (
                material_id INTEGER,
                name VARCHAR
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.dim_products (
                product_id INTEGER,
                name VARCHAR,
                pet_category_id INTEGER,
                category_id INTEGER,
                price DOUBLE,
                weight DOUBLE,
                color_id INTEGER,
                size VARCHAR,
                brand_id INTEGER,
                material_id INTEGER,
                description VARCHAR,
                rating DOUBLE, -- заменили FLOAT на DOUBLE
                reviews INTEGER,
                release_date DATE,
                expiry_date DATE,
                supplier_id INTEGER
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.snowflake.fact_sales (
                sale_id INTEGER,
                customer_id INTEGER,
                seller_id INTEGER,
                product_id INTEGER,
                store_id INTEGER,
                quantity INTEGER,
                total_price DOUBLE,
                date DATE
            )
        """);
    }
}
