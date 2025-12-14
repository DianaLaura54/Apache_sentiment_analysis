-- Snowflake Schema for Social Media Sentiment Analysis
-- This creates a complete data warehouse structure

-- Create database and schemas
CREATE DATABASE IF NOT EXISTS SOCIAL_MEDIA_DW;
USE DATABASE SOCIAL_MEDIA_DW;

-- Create schemas for different layers
CREATE SCHEMA IF NOT EXISTS RAW;           -- Raw ingested data
CREATE SCHEMA IF NOT EXISTS STAGING;      -- Cleaned and validated data
CREATE SCHEMA IF NOT EXISTS ANALYTICS;    -- Business-ready data marts
CREATE SCHEMA IF NOT EXISTS MONITORING;   -- Data quality and monitoring

-- =====================================================
-- RAW LAYER - Direct ingestion from Kafka/Spark
-- =====================================================

USE SCHEMA RAW;

-- Raw social media posts
CREATE TABLE IF NOT EXISTS raw_social_posts (
    id VARCHAR(100) PRIMARY KEY,
    text VARCHAR(5000),
    brand_mentioned VARCHAR(100),
    timestamp TIMESTAMP_NTZ,
    user_id VARCHAR(100),
    user_followers INTEGER,
    retweets INTEGER,
    likes INTEGER,
    replies INTEGER,
    location VARCHAR(200),
    platform VARCHAR(50),
    language VARCHAR(10),
    verified_user BOOLEAN,
    original_sentiment_score FLOAT,
    hashtags ARRAY,
    subreddit VARCHAR(100),
    upvotes INTEGER,
    downvotes INTEGER,
    comments INTEGER,
    processing_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ingestion_date DATE DEFAULT CURRENT_DATE()
);

-- Raw processed sentiment data from Spark
CREATE TABLE IF NOT EXISTS raw_sentiment_processed (
    id VARCHAR(100) PRIMARY KEY,
    text VARCHAR(5000),
    brand_mentioned VARCHAR(100),
    timestamp TIMESTAMP_NTZ,
    platform VARCHAR(50),
    calculated_sentiment FLOAT,
    sentiment_category VARCHAR(20),
    total_engagement INTEGER,
    engagement_rate FLOAT,
    location VARCHAR(200),
    processing_timestamp TIMESTAMP_NTZ,
    ingestion_date DATE DEFAULT CURRENT_DATE()
);

-- =====================================================
-- STAGING LAYER - Cleaned and validated data
-- =====================================================

USE SCHEMA STAGING;

-- Cleaned social media posts
CREATE TABLE IF NOT EXISTS staging_social_posts (
    post_id VARCHAR(100) PRIMARY KEY,
    post_text VARCHAR(5000) NOT NULL,
    brand_mentioned VARCHAR(100) NOT NULL,
    post_timestamp TIMESTAMP_NTZ NOT NULL,
    user_id VARCHAR(100),
    user_followers INTEGER,
    platform VARCHAR(50) NOT NULL,
    location_clean VARCHAR(200),
    country VARCHAR(100),
    region VARCHAR(100),
    city VARCHAR(100),
    language_code VARCHAR(10),
    is_verified_user BOOLEAN DEFAULT FALSE,
    total_engagement INTEGER DEFAULT 0,
    engagement_rate FLOAT DEFAULT 0.0,
    sentiment_score FLOAT,
    sentiment_category VARCHAR(20),
    processing_date DATE NOT NULL,
    data_quality_score FLOAT DEFAULT 1.0,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Brand master data
CREATE TABLE IF NOT EXISTS staging_brands (
    brand_id INTEGER AUTOINCREMENT PRIMARY KEY,
    brand_name VARCHAR(100) UNIQUE NOT NULL,
    industry VARCHAR(100),
    company_size VARCHAR(50),
    headquarters_country VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Platform master data
CREATE TABLE IF NOT EXISTS staging_platforms (
    platform_id INTEGER AUTOINCREMENT PRIMARY KEY,
    platform_name VARCHAR(50) UNIQUE NOT NULL,
    platform_type VARCHAR(50), -- social_media, forum, review_site
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- ANALYTICS LAYER - Business-ready data marts
-- =====================================================

USE SCHEMA ANALYTICS;

-- Daily brand sentiment metrics
CREATE TABLE IF NOT EXISTS daily_brand_sentiment (
    date_key DATE,
    brand_name VARCHAR(100),
    total_mentions INTEGER DEFAULT 0,
    unique_posts INTEGER DEFAULT 0,
    avg_sentiment_score FLOAT,
    sentiment_volatility FLOAT,
    positive_count INTEGER DEFAULT 0,
    negative_count INTEGER DEFAULT 0,
    neutral_count INTEGER DEFAULT 0,
    positive_ratio FLOAT DEFAULT 0.0,
    negative_ratio FLOAT DEFAULT 0.0,
    neutral_ratio FLOAT DEFAULT 0.0,
    total_engagement INTEGER DEFAULT 0,
    avg_engagement_per_post FLOAT DEFAULT 0.0,
    max_engagement INTEGER DEFAULT 0,
    twitter_mentions INTEGER DEFAULT 0,
    reddit_mentions INTEGER DEFAULT 0,
    instagram_mentions INTEGER DEFAULT 0,
    unique_locations INTEGER DEFAULT 0,
    engagement_rate FLOAT DEFAULT 0.0,
    sentiment_trend VARCHAR(20), -- improving, declining, stable
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (date_key, brand_name)
);

-- Hourly sentiment trends
CREATE TABLE IF NOT EXISTS hourly_sentiment_trends (
    date_key DATE,
    hour_of_day INTEGER,
    brand_name VARCHAR(100),
    mentions_count INTEGER DEFAULT 0,
    avg_sentiment FLOAT,
    hourly_engagement INTEGER DEFAULT 0,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (date_key, hour_of_day, brand_name)
);

-- Viral content tracking
CREATE TABLE IF NOT EXISTS viral_content (
    content_id VARCHAR(100) PRIMARY KEY,
    brand_name VARCHAR(100),
    post_text VARCHAR(5000),
    platform VARCHAR(50),
    sentiment_score FLOAT,
    sentiment_category VARCHAR(20),
    total_engagement INTEGER,
    engagement_rank INTEGER,
    post_date DATE,
    virality_score FLOAT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Sentiment anomalies
CREATE TABLE IF NOT EXISTS sentiment_anomalies (
    anomaly_id VARCHAR(150) PRIMARY KEY,
    brand_name VARCHAR(100),
    post_id VARCHAR(100),
    post_text VARCHAR(5000),
    sentiment_score FLOAT,
    z_score FLOAT,
    total_engagement INTEGER,
    anomaly_type VARCHAR(50), -- positive_spike, negative_spike
    detection_date DATE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Location-based sentiment insights
CREATE TABLE IF NOT EXISTS location_sentiment (
    location_key VARCHAR(250), -- brand_name + location
    brand_name VARCHAR(100),
    location VARCHAR(200),
    country VARCHAR(100),
    region VARCHAR(100),
    city VARCHAR(100),
    date_key DATE,
    mentions_count INTEGER DEFAULT 0,
    avg_sentiment FLOAT,
    total_engagement INTEGER DEFAULT 0,
    sentiment_category_dominant VARCHAR(20),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (location_key, date_key)
);

-- Brand ranking and comparison
CREATE TABLE IF NOT EXISTS brand_rankings (
    ranking_date DATE,
    brand_name VARCHAR(100),
    overall_rank INTEGER,
    sentiment_rank INTEGER,
    engagement_rank INTEGER,
    mention_volume_rank INTEGER,
    composite_score FLOAT,
    previous_rank INTEGER,
    rank_change INTEGER,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ranking_date, brand_name)
);

-- =====================================================
-- MONITORING LAYER - Data quality and pipeline health
-- =====================================================

USE SCHEMA MONITORING;

-- Data quality metrics
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    check_date DATE,
    table_name VARCHAR(100),
    total_records INTEGER,
    null_sentiment_count INTEGER DEFAULT 0,
    invalid_scores INTEGER DEFAULT 0,
    duplicate_records INTEGER DEFAULT 0,
    data_freshness_hours FLOAT,
    quality_score FLOAT,
    quality_status VARCHAR(20), -- PASS, WARN, FAIL
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (check_date, table_name)
);

-- Pipeline execution logs
CREATE TABLE IF NOT EXISTS pipeline_execution_log (
    execution_id VARCHAR(100) PRIMARY KEY,
    pipeline_name VARCHAR(100),
    execution_date DATE,
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    duration_seconds INTEGER,
    status VARCHAR(20), -- SUCCESS, FAILED, RUNNING
    records_processed INTEGER DEFAULT 0,
    error_message VARCHAR(5000),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- VIEWS FOR EASY ANALYTICS ACCESS
-- =====================================================

USE SCHEMA ANALYTICS;

-- Brand sentiment summary view
CREATE OR REPLACE VIEW vw_brand_sentiment_summary AS
SELECT
    b.brand_name,
    b.industry,
    d.date_key,
    d.total_mentions,
    d.avg_sentiment_score,
    d.positive_ratio,
    d.negative_ratio,
    d.total_engagement,
    d.sentiment_trend,
    CASE
        WHEN d.avg_sentiment_score > 0.3 THEN 'Very Positive'
        WHEN d.avg_sentiment_score > 0.1 THEN 'Positive'
        WHEN d.avg_sentiment_score > -0.1 THEN 'Neutral'
        WHEN d.avg_sentiment_score > -0.3 THEN 'Negative'
        ELSE 'Very Negative'
    END AS sentiment_label
FROM daily_brand_sentiment d
JOIN staging.staging_brands b ON d.brand_name = b.brand_name
WHERE d.date_key >= CURRENT_DATE - 30; -- Last 30 days

-- Top performing content view
CREATE OR REPLACE VIEW vw_top_viral_content AS
SELECT
    brand_name,
    post_text,
    platform,
    sentiment_category,
    total_engagement,
    engagement_rank,
    virality_score,
    post_date
FROM viral_content
WHERE engagement_rank <= 5 -- Top 5 per brand
AND post_date >= CURRENT_DATE - 7; -- Last week

-- Real-time sentiment dashboard view
CREATE OR REPLACE VIEW vw_sentiment_dashboard AS
WITH latest_metrics AS (
    SELECT
        brand_name,
        AVG(avg_sentiment_score) as avg_sentiment,
        SUM(total_mentions) as total_mentions,
        AVG(positive_ratio) as avg_positive_ratio,
        AVG(engagement_rate) as avg_engagement_rate
    FROM daily_brand_sentiment
    WHERE date_key >= CURRENT_DATE - 7
    GROUP BY brand_name
),
ranking_data AS (
    SELECT
        brand_name,
        overall_rank,
        rank_change
    FROM brand_rankings
    WHERE ranking_date = CURRENT_DATE - 1
)
SELECT
    l.brand_name,
    l.avg_sentiment,
    l.total_mentions,
    l.avg_positive_ratio,
    l.avg_engagement_rate,
    r.overall_rank,
    r.rank_change,
    CASE WHEN r.rank_change > 0 THEN '📈'
         WHEN r.rank_change < 0 THEN '📉'
         ELSE '➡️' END as trend_indicator
FROM latest_metrics l
LEFT JOIN ranking_data r ON l.brand_name = r.brand_name
ORDER BY l.avg_sentiment DESC;

-- =====================================================
-- STORED PROCEDURES FOR DATA LOADING
-- =====================================================

-- Procedure to load daily metrics from staging to analytics
CREATE OR REPLACE PROCEDURE load_daily_brand_metrics(date_param DATE)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Insert daily brand sentiment metrics
    INSERT INTO analytics.daily_brand_sentiment
    SELECT
        :date_param as date_key,
        brand_mentioned as brand_name,
        COUNT(*) as total_mentions,
        COUNT(DISTINCT post_id) as unique_posts,
        AVG(sentiment_score) as avg_sentiment_score,
        STDDEV(sentiment_score) as sentiment_volatility,
        SUM(CASE WHEN sentiment_category = 'positive' THEN 1 ELSE 0 END) as positive_count,
        SUM(CASE WHEN sentiment_category = 'negative' THEN 1 ELSE 0 END) as negative_count,
        SUM(CASE WHEN sentiment_category = 'neutral' THEN 1 ELSE 0 END) as neutral_count,
        SUM(CASE WHEN sentiment_category = 'positive' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as positive_ratio,
        SUM(CASE WHEN sentiment_category = 'negative' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as negative_ratio,
        SUM(CASE WHEN sentiment_category = 'neutral' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as neutral_ratio,
        SUM(total_engagement) as total_engagement,
        AVG(total_engagement) as avg_engagement_per_post,
        MAX(total_engagement) as max_engagement,
        SUM(CASE WHEN platform = 'twitter' THEN 1 ELSE 0 END) as twitter_mentions,
        SUM(CASE WHEN platform = 'reddit' THEN 1 ELSE 0 END) as reddit_mentions,
        SUM(CASE WHEN platform = 'instagram' THEN 1 ELSE 0 END) as instagram_mentions,
        COUNT(DISTINCT location_clean) as unique_locations,
        AVG(engagement_rate) as engagement_rate,
        CASE
            WHEN AVG(sentiment_score) > LAG(AVG(sentiment_score)) OVER (PARTITION BY brand_mentioned ORDER BY processing_date) THEN 'improving'
            WHEN AVG(sentiment_score) < LAG(AVG(sentiment_score)) OVER (PARTITION BY brand_mentioned ORDER BY processing_date) THEN 'declining'
            ELSE 'stable'
        END as sentiment_trend,
        CURRENT_TIMESTAMP() as created_at
    FROM staging.staging_social_posts
    WHERE processing_date = :date_param
    GROUP BY brand_mentioned;

    RETURN 'Daily brand metrics loaded successfully for ' || :date_param;
END;
$;

-- =====================================================
-- TASKS AND STREAMS FOR REAL-TIME PROCESSING
-- =====================================================

-- Create a stream to track changes in raw data
CREATE OR REPLACE STREAM raw_posts_stream ON TABLE raw.raw_social_posts;

-- Task to automatically process new data
CREATE OR REPLACE TASK process_new_posts
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('raw_posts_stream')
AS
    INSERT INTO staging.staging_social_posts (
        post_id,
        post_text,
        brand_mentioned,
        post_timestamp,
        user_id,
        user_followers,
        platform,
        location_clean,
        language_code,
        is_verified_user,
        total_engagement,
        engagement_rate,
        sentiment_score,
        sentiment_category,
        processing_date,
        data_quality_score
    )
    SELECT
        id,
        TRIM(text) as post_text,
        UPPER(TRIM(brand_mentioned)) as brand_mentioned,
        timestamp as post_timestamp,
        user_id,
        COALESCE(user_followers, 0) as user_followers,
        LOWER(platform) as platform,
        CASE
            WHEN location IS NOT NULL THEN TRIM(location)
            ELSE NULL
        END as location_clean,
        COALESCE(language, 'en') as language_code,
        COALESCE(verified_user, FALSE) as is_verified_user,
        COALESCE(retweets, 0) + COALESCE(likes, 0) + COALESCE(replies, 0) +
        COALESCE(upvotes, 0) + COALESCE(comments, 0) as total_engagement,
        CASE
            WHEN user_followers > 0 THEN
                (COALESCE(retweets, 0) + COALESCE(likes, 0) + COALESCE(replies, 0))::FLOAT / user_followers
            ELSE 0
        END as engagement_rate,
        original_sentiment_score as sentiment_score,
        CASE
            WHEN original_sentiment_score > 0.2 THEN 'positive'
            WHEN original_sentiment_score < -0.2 THEN 'negative'
            ELSE 'neutral'
        END as sentiment_category,
        ingestion_date as processing_date,
        CASE
            WHEN text IS NULL OR TRIM(text) = '' THEN 0.5
            WHEN brand_mentioned IS NULL OR TRIM(brand_mentioned) = '' THEN 0.6
            WHEN timestamp IS NULL THEN 0.7
            ELSE 1.0
        END as data_quality_score
    FROM raw_posts_stream
    WHERE METADATA$ACTION = 'INSERT'
    AND text IS NOT NULL
    AND brand_mentioned IS NOT NULL;

-- Enable the task
ALTER TASK process_new_posts RESUME;

-- =====================================================
-- SAMPLE DATA INSERTION FOR TESTING
-- =====================================================

-- Insert sample brand data
INSERT INTO staging.staging_brands (brand_name, industry, company_size, headquarters_country) VALUES
('Apple', 'Technology', 'Large', 'United States'),
('Google', 'Technology', 'Large', 'United States'),
('Microsoft', 'Technology', 'Large', 'United States'),
('Amazon', 'E-commerce/Technology', 'Large', 'United States'),
('Tesla', 'Automotive/Technology', 'Large', 'United States'),
('Netflix', 'Entertainment/Technology', 'Large', 'United States'),
('Spotify', 'Music/Technology', 'Medium', 'Sweden'),
('Nike', 'Apparel/Sports', 'Large', 'United States'),
('Starbucks', 'Food & Beverage', 'Large', 'United States'),
('McDonald''s', 'Food & Beverage', 'Large', 'United States');

-- Insert sample platform data
INSERT INTO staging.staging_platforms (platform_name, platform_type) VALUES
('twitter', 'social_media'),
('reddit', 'forum'),
('instagram', 'social_media'),
('facebook', 'social_media'),
('linkedin', 'professional_network'),
('youtube', 'video_platform'),
('tiktok', 'social_media');

-- =====================================================
-- ANALYTICAL QUERIES FOR INSIGHTS
-- =====================================================

-- Top brands by sentiment over time
CREATE OR REPLACE VIEW vw_brand_sentiment_trends AS
SELECT
    brand_name,
    date_key,
    avg_sentiment_score,
    total_mentions,
    LAG(avg_sentiment_score, 1) OVER (PARTITION BY brand_name ORDER BY date_key) as prev_sentiment,
    avg_sentiment_score - LAG(avg_sentiment_score, 1) OVER (PARTITION BY brand_name ORDER BY date_key) as sentiment_change,
    ROW_NUMBER() OVER (PARTITION BY date_key ORDER BY avg_sentiment_score DESC) as daily_rank
FROM analytics.daily_brand_sentiment
WHERE date_key >= CURRENT_DATE - 30;

-- Engagement vs Sentiment correlation
CREATE OR REPLACE VIEW vw_engagement_sentiment_correlation AS
SELECT
    brand_name,
    date_key,
    avg_sentiment_score,
    avg_engagement_per_post,
    total_mentions,
    CASE
        WHEN avg_sentiment_score > 0.3 AND avg_engagement_per_post > 100 THEN 'High Sentiment, High Engagement'
        WHEN avg_sentiment_score > 0.3 AND avg_engagement_per_post <= 100 THEN 'High Sentiment, Low Engagement'
        WHEN avg_sentiment_score <= 0.3 AND avg_engagement_per_post > 100 THEN 'Low Sentiment, High Engagement'
        ELSE 'Low Sentiment, Low Engagement'
    END as engagement_sentiment_quadrant
FROM analytics.daily_brand_sentiment
WHERE date_key >= CURRENT_DATE - 7;

-- Platform performance analysis
CREATE OR REPLACE VIEW vw_platform_analysis AS
WITH platform_metrics AS (
    SELECT
        platform,
        brand_mentioned,
        COUNT(*) as post_count,
        AVG(sentiment_score) as avg_sentiment,
        AVG(total_engagement) as avg_engagement,
        AVG(engagement_rate) as avg_engagement_rate
    FROM staging.staging_social_posts
    WHERE processing_date >= CURRENT_DATE - 7
    GROUP BY platform, brand_mentioned
)
SELECT
    platform,
    brand_mentioned,
    post_count,
    avg_sentiment,
    avg_engagement,
    avg_engagement_rate,
    RANK() OVER (PARTITION BY platform ORDER BY avg_sentiment DESC) as sentiment_rank_on_platform,
    RANK() OVER (PARTITION BY brand_mentioned ORDER BY avg_sentiment DESC) as platform_rank_for_brand
FROM platform_metrics;

-- =====================================================
-- DATA RETENTION AND ARCHIVAL POLICIES
-- =====================================================

-- Create retention policy for raw data (keep 90 days)
CREATE OR REPLACE TASK cleanup_old_raw_data
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 2 * * 0'  -- Weekly on Sunday at 2 AM
AS
    DELETE FROM raw.raw_social_posts
    WHERE ingestion_date < CURRENT_DATE - 90;

-- Create retention policy for monitoring data (keep 1 year)
CREATE OR REPLACE TASK cleanup_old_monitoring_data
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 3 1 * *'  -- Monthly on 1st at 3 AM
AS
    DELETE FROM monitoring.pipeline_execution_log
    WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '1 year';

-- Enable cleanup tasks
ALTER TASK cleanup_old_raw_data RESUME;
ALTER TASK cleanup_old_monitoring_data RESUME;

-- =====================================================
-- PERFORMANCE OPTIMIZATION
-- =====================================================

-- Add clustering keys for better query performance
ALTER TABLE analytics.daily_brand_sentiment CLUSTER BY (date_key, brand_name);
ALTER TABLE staging.staging_social_posts CLUSTER BY (processing_date, brand_mentioned);
ALTER TABLE analytics.viral_content CLUSTER BY (post_date, brand_name);

-- Create indexes for frequently queried columns
-- Note: Snowflake automatically manages indexes, but we can optimize with clustering

-- =====================================================
-- GRANTS AND SECURITY
-- =====================================================

-- Create roles for different user types
CREATE ROLE IF NOT EXISTS data_analyst;
CREATE ROLE IF NOT EXISTS data_engineer;
CREATE ROLE IF NOT EXISTS business_user;

-- Grant permissions to data analyst role
GRANT USAGE ON DATABASE social_media_dw TO ROLE data_analyst;
GRANT USAGE ON SCHEMA analytics TO ROLE data_analyst;
GRANT USAGE ON SCHEMA staging TO ROLE data_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO ROLE data_analyst;
GRANT SELECT ON ALL VIEWS IN SCHEMA analytics TO ROLE data_analyst;

-- Grant permissions to data engineer role
GRANT USAGE ON DATABASE social_media_dw TO ROLE data_engineer;
GRANT USAGE ON ALL SCHEMAS IN DATABASE social_media_dw TO ROLE data_engineer;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE social_media_dw TO ROLE data_engineer;
GRANT ALL PRIVILEGES ON ALL VIEWS IN DATABASE social_media_dw TO ROLE data_engineer;

-- Grant limited permissions to business user role
GRANT USAGE ON DATABASE social_media_dw TO ROLE business_user;
GRANT USAGE ON SCHEMA analytics TO ROLE business_user;
GRANT SELECT ON ALL VIEWS IN SCHEMA analytics TO ROLE business_user;

-- =====================================================
-- MONITORING AND ALERTING SETUP
-- =====================================================

-- Create alerts for data quality issues
CREATE OR REPLACE ALERT data_quality_alert
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '10 MINUTE'
    IF (EXISTS (
        SELECT 1 FROM monitoring.data_quality_metrics
        WHERE check_date = CURRENT_DATE
        AND quality_status = 'FAIL'
    ))
    THEN CALL send_notification('Data quality alert: Quality checks failed for today');

-- Create alert for pipeline failures
CREATE OR REPLACE ALERT pipeline_failure_alert
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    IF (EXISTS (
        SELECT 1 FROM monitoring.pipeline_execution_log
        WHERE execution_date = CURRENT_DATE
        AND status = 'FAILED'
        AND created_at >= CURRENT_TIMESTAMP - INTERVAL '10 minutes'
    ))
    THEN CALL send_notification('Pipeline failure alert: A pipeline execution has failed');

-- Resume alerts
ALTER ALERT data_quality_alert RESUME;
ALTER ALERT pipeline_failure_alert RESUME;