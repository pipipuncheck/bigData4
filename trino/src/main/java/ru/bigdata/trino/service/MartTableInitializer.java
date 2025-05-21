package ru.bigdata.trino.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class MartTableInitializer {
    private final JdbcTemplate jdbcTemplate;

    public MartTableInitializer(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void createSalesMarts() {
        jdbcTemplate.execute("CREATE SCHEMA IF NOT EXISTS clickhouse.sales_mart");

        // Продукты
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.products_most_saling (
                product_id BIGINT,
                product_name VARCHAR,
                sales_count BIGINT
            )
        """);
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.products_total_price_by_categories (
                category_id BIGINT,
                category_name VARCHAR,
                total_price DECIMAL(18,2)
            )
        """);
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.products_average_rating_and_reviews (
                product_id BIGINT,
                product_name VARCHAR,
                rating DECIMAL(10,1),
                reviews BIGINT
            )
        """);

        // Клиенты
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.customers_most_buying (
                customer_id BIGINT,
                customer_first_name VARCHAR,
                customer_last_name VARCHAR,
                customer_email VARCHAR,
                total_price DECIMAL(18,2)
            )
        """);
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.customers_distribution_by_countries (
                country VARCHAR,
                customers_quantity BIGINT,
                share DECIMAL(18,2)
            )
        """);
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.customers_average_price (
                customer_id BIGINT,
                customer_first_name VARCHAR,
                customer_last_name VARCHAR,
                average_price DECIMAL(18,2)
            )
        """);

        // Время
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.time_monthly_trends (
                month DATE,
                total_price DECIMAL(18,2),
                sales_count BIGINT
            )
        """);
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.time_month_over_month (
                month DATE,
                total_price DECIMAL(18,2),
                sales_count BIGINT,
                m_o_m_change DECIMAL(18,2),
                m_o_m_change_share DECIMAL(18,2)
            )
        """);
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.time_average_sales_by_month (
                month DATE,
                average_sales_quantity DECIMAL(18,2)
            )
        """);

        // Магазины
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.stores_top5_by_price (
                store_id BIGINT,
                store_name VARCHAR,
                total_price DECIMAL(18,2)
            )
        """);
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.stores_sales_distribution_by_countries (
                country VARCHAR,
                sales_quantity BIGINT,
                share DECIMAL(18,2)
            )
        """);
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.stores_average_price (
                store_id BIGINT,
                store_name VARCHAR,
                average_price DECIMAL(18,2)
            )
        """);

        // Поставщики
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.suppliers_top5_by_price (
                supplier_id BIGINT,
                supplier_name VARCHAR,
                total_price DECIMAL(18,2)
            )
        """);
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.suppliers_average_product_price (
                supplier_id BIGINT,
                supplier_name VARCHAR,
                average_product_price DECIMAL(18,2)
            )
        """);
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.suppliers_sales_distribution_by_countries (
                suppliers_country VARCHAR,
                sales_quantity BIGINT,
                share DECIMAL(18,2)
            )
        """);

        // Качество продукции
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.quality_most_rating_products (
                product_id BIGINT,
                product_name VARCHAR,
                rating DECIMAL(18,2)
            )
        """);
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.quality_least_rating_products (
                product_id BIGINT,
                product_name VARCHAR,
                rating DECIMAL(18,2)
            )
        """);
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.quality_rating_sales_correlation (
                correlation DECIMAL(18,2)
            )
        """);
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clickhouse.sales_mart.quality_most_reviewed_products (
                product_id BIGINT,
                product_name VARCHAR,
                reviews BIGINT
            )
        """);
    }
}
