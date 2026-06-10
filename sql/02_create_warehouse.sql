-- ============================
-- STAR SCHEMA: Gobest Cab DW 
-- ============================
SET FOREIGN_KEY_CHECKS = 0;

DROP DATABASE IF EXISTS warehouse;

CREATE DATABASE IF NOT EXISTS warehouse
  CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

USE warehouse;

-- ============================
-- Drop existing tables (FK-safe order)
-- ============================
DROP TABLE IF EXISTS Fact_Label;
DROP TABLE IF EXISTS Dim_Sensor;
DROP TABLE IF EXISTS Dim_Driver;

-- ============================
-- Dimension: Driver + Car
-- ============================
CREATE TABLE IF NOT EXISTS Dim_Driver (
    driver_key INT PRIMARY KEY AUTO_INCREMENT,      -- surrogate key
    driver_id INT,                                  -- natural key from source

    -- Driver attributes
    driver_name VARCHAR(100),
    date_of_birth DATE,
    gender VARCHAR(10),
    no_of_years_driving_exp INT,
    rating DECIMAL(2,1),

    -- Car attributes now stored together with driver
    car_brand VARCHAR(100) NOT NULL,
    car_model_year INT,

    -- Persisted outlier flags/scores
    no_of_years_driving_exp__is_uni_outlier TINYINT(1) DEFAULT NULL,
    rating__is_uni_outlier TINYINT(1) DEFAULT NULL,
    car_model_year__is_uni_outlier TINYINT(1) DEFAULT NULL,
    mv_score DECIMAL(10,6) DEFAULT NULL,
    mv_is_outlier TINYINT(1) DEFAULT NULL,

    date_cleaned DATETIME DEFAULT NULL,

    -- Ensure one row per driver-car combo
    CONSTRAINT uk_driver_car UNIQUE (driver_id, car_brand, car_model_year)
);

-- ============================
-- Dimension: Sensor (Trip-level feature profile)
-- One row per trip (booking_id) with all FE_* attributes
-- ============================
CREATE TABLE IF NOT EXISTS Dim_Sensor (
    sensor_key INT PRIMARY KEY AUTO_INCREMENT,   -- surrogate key

    -- Natural/business key
    booking_id BIGINT NOT NULL,                  -- from sensor_data.bookingID

    -- ========== ORIGINAL SENSOR COLUMNS (aggregated per trip, but same names) ==========
    accuracy        FLOAT,       -- accuracy inferred by GPS in meters
    bearing         FLOAT,       -- GPS bearing in degree
    acceleration_x  FLOAT,       -- accelerometer x axis (m/s^2)
    acceleration_y  FLOAT,       -- accelerometer y axis (m/s^2)
    acceleration_z  FLOAT,       -- accelerometer z axis (m/s^2)
    gyro_x          FLOAT,       -- gyroscope x axis (rad/s)
    gyro_y          FLOAT,       -- gyroscope y axis (rad/s)
    gyro_z          FLOAT,       -- gyroscope z axis (rad/s)
    second          INT,         -- trip-level representation of time; e.g. duration or last second
    speed           FLOAT,       -- trip-level representation of speed (e.g. average)

    -- ========== FEATURE-ENGINEERED TRIP-LEVEL METRICS (FE_*) ==========

    -- Core trip meta
    FE_n_samples          INT,                -- number of sensor records in trip
    FE_trip_duration_s    INT,                -- max(second) - min(second)
    FE_sampling_rate_hz   FLOAT,              -- FE_n_samples / FE_trip_duration_s

    -- Speed behaviour
    FE_speed_mean             FLOAT,
    FE_speed_std              FLOAT,
    FE_speed_max              FLOAT,
    FE_speed_min              FLOAT,
    FE_speed_p90              FLOAT,
    FE_speed_change_rate_med  FLOAT,

    -- Acceleration magnitude (from acc_magnitude)
    FE_acc_mag_mean       FLOAT,
    FE_acc_mag_std        FLOAT,
    FE_acc_mag_max        FLOAT,
    FE_acc_mag_p95        FLOAT,

    -- Axis-level acceleration (x, y, z)
    FE_acc_x_mean         FLOAT,
    FE_acc_y_mean         FLOAT,
    FE_acc_z_mean         FLOAT,
    FE_acc_x_std          FLOAT,
    FE_acc_y_std          FLOAT,
    FE_acc_z_std          FLOAT,

    -- Jerk (from |Δacc_magnitude| / Δt)
    FE_jerk_med           FLOAT,
    FE_jerk_p95           FLOAT,

    -- Gyro magnitude (from gyro_magnitude)
    FE_gyro_mag_mean      FLOAT,
    FE_gyro_mag_std       FLOAT,
    FE_gyro_mag_p95       FLOAT,

    -- Turning behaviour (bearing-based)
    FE_turn_delta_std_deg   FLOAT,
    FE_turn_rate_med_deg_s  FLOAT,

    -- GPS accuracy (engineered)
    FE_gps_acc_mean       FLOAT,
    FE_gps_acc_std        FLOAT,

    -- Start / End dynamics (engineered)
    FE_start_speed        FLOAT,             -- speed at earliest second
    FE_end_speed          FLOAT,             -- speed at latest second

    -- ETL metadata for the profile
    date_cleaned      DATETIME DEFAULT NULL,

    -- Ensure one sensor profile per trip
    CONSTRAINT uk_dim_sensor_booking UNIQUE (booking_id)
);

-- ============================
-- Fact Table: Label (Safety as Fact)
-- Grain: 1 row per trip (booking_id)
-- ============================
CREATE TABLE IF NOT EXISTS Fact_Label (
    -- Surrogate key for the fact
    label_key       BIGINT PRIMARY KEY AUTO_INCREMENT,

    -- Natural trip key from sensor/safety data
    booking_id      BIGINT NOT NULL,

    -- Dimension foreign keys
    driver_key      INT,        -- FK to Dim_Driver
    sensor_key      INT,        -- FK to Dim_Sensor

    -- Safety label (fact)
    label           TINYINT,    -- 0 = Safe, 1 = Dangerous (nullable)

    -- Optional: date of ETL / processing
    date_cleaned    DATETIME DEFAULT NULL,

    -- Enforce that each booking appears once at this grain
    CONSTRAINT uk_fact_label_booking UNIQUE (booking_id),

    -- Foreign-key constraints
    CONSTRAINT fk_factlabel_driver FOREIGN KEY (driver_key)
        REFERENCES Dim_Driver(driver_key)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    CONSTRAINT fk_factlabel_sensor FOREIGN KEY (sensor_key)
        REFERENCES Dim_Sensor(sensor_key)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- ============================
-- Helpful indexes
-- ============================
CREATE INDEX idx_factlabel_driver ON Fact_Label (driver_key);
CREATE INDEX idx_factlabel_sensor ON Fact_Label (sensor_key);
CREATE INDEX idx_factlabel_label  ON Fact_Label (label);

SELECT * FROM Fact_Label