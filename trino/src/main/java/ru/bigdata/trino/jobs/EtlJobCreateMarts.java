package ru.bigdata.trino.jobs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import ru.bigdata.trino.service.MartTableInitializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class EtlJobCreateMarts {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private MartTableInitializer tableInitializer;

    public void createMarts() {
        tableInitializer.createSalesMarts();
        populateCustomerMarts();
        populateProductMarts();
        populateQualityMarts();
        populateStoreMarts();
        populateTimeMarts();
        populateSupplierMarts();

    }

    public void populateCustomerMarts() {
        // 1. ТОП 10 клиентов по общей сумме покупок
        jdbcTemplate.execute("""
            INSERT INTO clickhouse.sales_mart.customers_most_buying
            SELECT 
                c.customer_id,
                c.first_name AS customer_first_name,
                c.last_name AS customer_last_name,
                c.email AS customer_email,
                SUM(s.total_price) AS total_price
            FROM clickhouse.snowflake.fact_sales s
            JOIN clickhouse.snowflake.dim_customers c ON s.customer_id = c.customer_id
            GROUP BY c.customer_id, c.first_name, c.last_name, c.email
            ORDER BY total_price DESC
            LIMIT 10
        """);

        // 2. Распределение клиентов по странам
        jdbcTemplate.execute("""
            INSERT INTO clickhouse.sales_mart.customers_distribution_by_countries
            WITH country_customer_counts AS (
                SELECT 
                    co.name AS country,
                    COUNT(DISTINCT c.customer_id) AS customers_quantity
                FROM clickhouse.snowflake.dim_customers c
                JOIN clickhouse.snowflake.dim_countries co ON c.country_id = co.country_id
                GROUP BY co.name
            ),
            total_customers AS (
                SELECT COUNT(DISTINCT customer_id) AS total FROM clickhouse.snowflake.dim_customers
            )
            SELECT 
                ccc.country,
                ccc.customers_quantity,
                ROUND(ccc.customers_quantity / t.total * 100, 2) AS share
            FROM country_customer_counts ccc
            CROSS JOIN total_customers t
        """);

        // 3. Средний чек клиента
        jdbcTemplate.execute("""
            INSERT INTO clickhouse.sales_mart.customers_average_price
            SELECT 
                c.customer_id,
                c.first_name AS customer_first_name,
                c.last_name AS customer_last_name,
                ROUND(AVG(s.total_price), 2) AS average_price
            FROM clickhouse.snowflake.fact_sales s
            JOIN clickhouse.snowflake.dim_customers c ON s.customer_id = c.customer_id
            GROUP BY c.customer_id, c.first_name, c.last_name
        """);
    }

    public void populateProductMarts() {
        jdbcTemplate.execute("""
            INSERT INTO clickhouse.sales_mart.products_most_saling
            SELECT 
                p.product_id,
                p.name AS product_name,
                SUM(s.quantity) AS sales_count
            FROM clickhouse.snowflake.fact_sales s
            JOIN clickhouse.snowflake.dim_products p ON s.product_id = p.product_id
            GROUP BY p.product_id, p.name
            ORDER BY sales_count DESC
            LIMIT 10
        """);

        jdbcTemplate.execute("""
            INSERT INTO clickhouse.sales_mart.products_total_price_by_categories
            SELECT 
                c.category_id,
                c.name AS category_name,
                SUM(s.total_price) AS total_price
            FROM clickhouse.snowflake.fact_sales s
            JOIN clickhouse.snowflake.dim_products p ON s.product_id = p.product_id
            JOIN clickhouse.snowflake.dim_product_categories c ON p.category_id = c.category_id
            GROUP BY c.category_id, c.name
        """);

        jdbcTemplate.execute("""
            INSERT INTO clickhouse.sales_mart.products_average_rating_and_reviews
            SELECT 
                product_id,
                name AS product_name,
                rating,
                reviews
            FROM clickhouse.snowflake.dim_products
        """);
    }

    public void populateSupplierMarts() {
        jdbcTemplate.execute("""
            INSERT INTO clickhouse.sales_mart.suppliers_top5_by_price
            SELECT 
                sup.supplier_id,
                sup.name AS supplier_name,
                SUM(s.total_price) AS total_price
            FROM clickhouse.snowflake.fact_sales s
            JOIN clickhouse.snowflake.dim_products p ON s.product_id = p.product_id
            JOIN clickhouse.snowflake.dim_suppliers sup ON p.supplier_id = sup.supplier_id
            GROUP BY sup.supplier_id, sup.name
            ORDER BY total_price DESC
            LIMIT 5
        """);

        jdbcTemplate.execute("""
            INSERT INTO clickhouse.sales_mart.suppliers_average_product_price
            SELECT 
                sup.supplier_id,
                sup.name AS supplier_name,
                AVG(p.price) AS average_product_price
            FROM clickhouse.snowflake.dim_products p
            JOIN clickhouse.snowflake.dim_suppliers sup ON p.supplier_id = sup.supplier_id
            GROUP BY sup.supplier_id, sup.name
        """);

        jdbcTemplate.execute("""
            INSERT INTO clickhouse.sales_mart.suppliers_sales_distribution_by_countries
            WITH sales_by_country AS (
                SELECT 
                    sup.country_id,
                    SUM(s.quantity) AS sales_quantity
                FROM clickhouse.snowflake.fact_sales s
                JOIN clickhouse.snowflake.dim_products p ON s.product_id = p.product_id
                JOIN clickhouse.snowflake.dim_suppliers sup ON p.supplier_id = sup.supplier_id
                GROUP BY sup.country_id
            ),
            total_sales AS (
                SELECT SUM(quantity) AS total_quantity FROM clickhouse.snowflake.fact_sales
            )
            SELECT 
                c.name AS suppliers_country,
                s.sales_quantity,
                ROUND(s.sales_quantity / t.total_quantity * 100, 2) AS share
            FROM sales_by_country s
            JOIN clickhouse.snowflake.dim_countries c ON s.country_id = c.country_id
            CROSS JOIN total_sales t
        """);
    }

    public void populateStoreMarts() {
        jdbcTemplate.execute("""
            INSERT INTO clickhouse.sales_mart.stores_top5_by_price
            SELECT 
                st.store_id,
                st.name AS store_name,
                SUM(s.total_price) AS total_price
            FROM clickhouse.snowflake.fact_sales s
            JOIN clickhouse.snowflake.dim_stores st ON s.store_id = st.store_id
            GROUP BY st.store_id, st.name
            ORDER BY total_price DESC
            LIMIT 5
        """);

        jdbcTemplate.execute("""
            INSERT INTO clickhouse.sales_mart.stores_sales_distribution_by_countries
            WITH sales_by_country AS (
                SELECT st.country_id, SUM(s.quantity) AS sales_quantity
                FROM clickhouse.snowflake.fact_sales s
                JOIN clickhouse.snowflake.dim_stores st ON s.store_id = st.store_id
                GROUP BY st.country_id
            ),
            total_sales AS (
                SELECT SUM(quantity) AS total_quantity FROM clickhouse.snowflake.fact_sales
            )
            SELECT 
                c.name AS country,
                s.sales_quantity,
                ROUND(s.sales_quantity / t.total_quantity * 100, 2) AS share
            FROM sales_by_country s
            JOIN clickhouse.snowflake.dim_countries c ON s.country_id = c.country_id
            CROSS JOIN total_sales t
        """);

        jdbcTemplate.execute("""
            INSERT INTO clickhouse.sales_mart.stores_average_price
            SELECT 
                st.store_id,
                st.name AS store_name,
                ROUND(AVG(s.total_price), 2) AS average_price
            FROM clickhouse.snowflake.fact_sales s
            JOIN clickhouse.snowflake.dim_stores st ON s.store_id = st.store_id
            GROUP BY st.store_id, st.name
        """);
    }

    public void populateTimeMarts() {
        jdbcTemplate.execute("""
            INSERT INTO clickhouse.sales_mart.time_monthly_trends
            SELECT 
                date_trunc('month', s.date) AS month,
                SUM(s.total_price) AS total_price,
                SUM(s.quantity) AS sales_count
            FROM clickhouse.snowflake.fact_sales s
            GROUP BY date_trunc('month', s.date)
            ORDER BY date_trunc('month', s.date)
        """);

        jdbcTemplate.execute("""
            INSERT INTO clickhouse.sales_mart.time_month_over_month
            WITH monthly AS (
                SELECT 
                    date_trunc('month', s.date) AS month,
                    SUM(s.total_price) AS total_price,
                    SUM(s.quantity) AS sales_count
                FROM clickhouse.snowflake.fact_sales s
                GROUP BY date_trunc('month', s.date)
            ),
            lagged AS (
                SELECT 
                    month,
                    LAG(total_price) OVER (ORDER BY month) AS prev_total_price
                FROM monthly
            )
            SELECT 
                m.month,
                m.total_price,
                m.sales_count,
                m.total_price - l.prev_total_price AS m_o_m_change,
                ROUND((m.total_price - l.prev_total_price) / m.total_price * 100, 2) AS m_o_m_change_share
            FROM monthly m
            JOIN lagged l ON m.month = l.month
        """);

        jdbcTemplate.execute("""
            INSERT INTO clickhouse.sales_mart.time_average_sales_by_month
            SELECT 
                date_trunc('month', s.date) AS month,
                ROUND(AVG(s.quantity), 2) AS average_sales_quantity
            FROM clickhouse.snowflake.fact_sales s
            GROUP BY date_trunc('month', s.date)
            ORDER BY date_trunc('month', s.date)
        """);
    }

    public void populateQualityMarts() {
        jdbcTemplate.execute("""
        INSERT INTO clickhouse.sales_mart.quality_most_rating_products
        SELECT 
            product_id,
            name AS product_name,
            rating
        FROM clickhouse.snowflake.dim_products
        ORDER BY rating DESC
        LIMIT 1
    """);

        jdbcTemplate.execute("""
        INSERT INTO clickhouse.sales_mart.quality_least_rating_products
        SELECT 
            product_id,
            name AS product_name,
            rating
        FROM clickhouse.snowflake.dim_products
        ORDER BY rating ASC
        LIMIT 1
    """);

        jdbcTemplate.execute("""
        INSERT INTO clickhouse.sales_mart.quality_most_reviewed_products
        SELECT 
            product_id,
            name AS product_name,
            reviews
        FROM clickhouse.snowflake.dim_products
        ORDER BY reviews DESC
    """);

        // Выборка рейтингов и суммарных продаж
        List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
        SELECT p.rating, SUM(s.quantity) AS sales_quantity
        FROM clickhouse.snowflake.dim_products p
        JOIN clickhouse.snowflake.fact_sales s ON p.product_id = s.product_id
        WHERE p.rating IS NOT NULL
        GROUP BY p.rating
    """);

        List<Double> ratings = new ArrayList<>();
        List<Double> quantities = new ArrayList<>();

        for (Map<String, Object> row : rows) {
            Number rating = (Number) row.get("rating");
            Number salesQuantity = (Number) row.get("sales_quantity");
            if (rating != null && salesQuantity != null) {
                ratings.add(rating.doubleValue());
                quantities.add(salesQuantity.doubleValue());
            }
        }

        double correlation = computePearsonCorrelation(ratings, quantities);

        jdbcTemplate.update("""
        INSERT INTO clickhouse.sales_mart.quality_rating_sales_correlation (correlation)
        VALUES (?)
    """, correlation);
    }

    private double computePearsonCorrelation(List<Double> xList, List<Double> yList) {
        int n = xList.size();
        if (n == 0 || n != yList.size()) return 0.0;

        double sumX = 0, sumY = 0, sumX2 = 0, sumY2 = 0, sumXY = 0;

        for (int i = 0; i < n; i++) {
            double x = xList.get(i);
            double y = yList.get(i);

            sumX += x;
            sumY += y;
            sumX2 += x * x;
            sumY2 += y * y;
            sumXY += x * y;
        }

        double numerator = sumXY - (sumX * sumY / n);
        double denominator = Math.sqrt(
                (sumX2 - (sumX * sumX / n)) *
                        (sumY2 - (sumY * sumY / n))
        );

        return denominator == 0 ? 0.0 : numerator / denominator;
    }
}
