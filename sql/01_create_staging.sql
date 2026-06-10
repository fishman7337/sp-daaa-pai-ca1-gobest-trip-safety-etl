-- ============================================
-- STAGING DB + TABLES + VIEWS + BULK LOADS  (RDS-safe; cleaned_date stays NULL)
-- ============================================

-- 0) DB
CREATE DATABASE IF NOT EXISTS staging
  CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE staging;

-- 1) Drop clean views if exist
DROP TABLE IF EXISTS sensor_data_source;
DROP VIEW IF EXISTS sensor_data;
DROP VIEW IF EXISTS drivers;
DROP VIEW IF EXISTS safety_labels;

-- 2) (Re)create STAGING TABLES (raw mirrors) + cleaned_date
CREATE TABLE IF NOT EXISTS sensor_data_source (
  csv_index      VARCHAR(64),
  booking_id     VARCHAR(64),
  accuracy       VARCHAR(64),
  bearing        VARCHAR(64),
  acceleration_x VARCHAR(64),
  acceleration_y VARCHAR(64),
  acceleration_z VARCHAR(64),
  gyro_x         VARCHAR(64),
  gyro_y         VARCHAR(64),
  gyro_z         VARCHAR(64),
  second         VARCHAR(64),
  speed          VARCHAR(64),
  cleaned_date   DATETIME DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS drivers_source (
  driver_id                 VARCHAR(64),
  driver_name               VARCHAR(255),
  data_of_birth             VARCHAR(64),
  No_of_Years_driving_exp   VARCHAR(64),
  gender                    VARCHAR(32),
  car_brand                 VARCHAR(64),
  car_model_year            VARCHAR(64),
  driver_rating             VARCHAR(64),
  cleaned_date              DATETIME DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS safety_labels_source (
  booking_id   VARCHAR(64),
  driver_id    VARCHAR(64),
  label        VARCHAR(16),
  cleaned_date DATETIME DEFAULT NULL
);

-- 3) CLEAN VIEWS (read-only)
CREATE OR REPLACE VIEW sensor_data AS
SELECT
  CASE WHEN booking_id IS NULL OR TRIM(booking_id) = ''
       THEN NULL ELSE CAST(CAST(booking_id AS DECIMAL(20,0)) AS UNSIGNED) END AS `bookingID`,
  CAST(NULLIF(TRIM(accuracy),       '') AS DECIMAL(18,6)) AS `Accuracy`,
  CAST(NULLIF(TRIM(bearing),        '') AS DECIMAL(18,6)) AS `Bearing`,
  CAST(NULLIF(TRIM(acceleration_x), '') AS DECIMAL(18,6)) AS `acceleration_x`,
  CAST(NULLIF(TRIM(acceleration_y), '') AS DECIMAL(18,6)) AS `acceleration_y`,
  CAST(NULLIF(TRIM(acceleration_z), '') AS DECIMAL(18,6)) AS `acceleration_z`,
  CAST(NULLIF(TRIM(gyro_x),         '') AS DECIMAL(18,6)) AS `gyro_x`,
  CAST(NULLIF(TRIM(gyro_y),         '') AS DECIMAL(18,6)) AS `gyro_y`,
  CAST(NULLIF(TRIM(gyro_z),         '') AS DECIMAL(18,6)) AS `gyro_z`,
  CAST(NULLIF(TRIM(second),         '') AS UNSIGNED)       AS `second`,
  CAST(NULLIF(TRIM(speed),          '') AS DECIMAL(18,6))  AS `Speed`,
  csv_index AS `_csv_index`,
  cleaned_date
FROM sensor_data_source;

CREATE OR REPLACE VIEW drivers AS
SELECT
  CAST(NULLIF(TRIM(driver_id), '') AS UNSIGNED) AS id,
  NULLIF(TRIM(driver_name), '')                 AS name,
  CASE WHEN data_of_birth IS NULL OR TRIM(data_of_birth) = ''
       THEN NULL ELSE STR_TO_DATE(TRIM(data_of_birth), '%m/%d/%Y') END AS date_of_birth,
  CAST(NULLIF(TRIM(No_of_Years_driving_exp), '') AS SIGNED) AS no_of_years_driving_exp,
  NULLIF(TRIM(gender), '')  AS gender,
  NULLIF(TRIM(car_brand), '') AS car_make,
  CAST(NULLIF(TRIM(car_model_year), '') AS SIGNED) AS car_model_year,
  CAST(NULLIF(TRIM(driver_rating), '') AS DECIMAL(4,2)) AS rating,
  cleaned_date
FROM drivers_source;

CREATE OR REPLACE VIEW safety_labels AS
SELECT
  CASE WHEN booking_id IS NULL OR TRIM(booking_id) = ''
       THEN NULL ELSE CAST(CAST(booking_id AS DECIMAL(20,0)) AS UNSIGNED) END AS booking_id,
  CAST(NULLIF(TRIM(driver_id), '') AS UNSIGNED) AS driver_id,
  CAST(NULLIF(TRIM(label),     '') AS UNSIGNED) AS label,
  cleaned_date
FROM safety_labels_source;

-- 4) Session-level toggles
SET autocommit = 0;
SET foreign_key_checks = 0;
SET unique_checks = 0;

-- 5) TRUNCATE STAGING (idempotent reload)
TRUNCATE TABLE sensor_data_source;
TRUNCATE TABLE drivers_source;
TRUNCATE TABLE safety_labels_source;

-- 6) LOADS — keep cleaned_date = NULL on ingest

-- Update these client-side paths before running the load script. Raw dataset
-- files are intentionally not committed to this repository.

-- ---------- SENSOR PARTS ----------
LOAD DATA LOCAL INFILE
  'data/raw/driving_dataset/Sensor_Dataset_features/0_Sensor_DataSet_features_part.csv'
INTO TABLE sensor_data_source
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(csv_index, booking_id, accuracy, bearing,
 acceleration_x, acceleration_y, acceleration_z,
 gyro_x, gyro_y, gyro_z, second, speed)
SET cleaned_date = NULL;

LOAD DATA LOCAL INFILE
  'data/raw/driving_dataset/Sensor_Dataset_features/1-Sensor_DataSet_features_part.csv'
INTO TABLE sensor_data_source
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(csv_index, booking_id, accuracy, bearing,
 acceleration_x, acceleration_y, acceleration_z,
 gyro_x, gyro_y, gyro_z, second, speed)
SET cleaned_date = NULL;

LOAD DATA LOCAL INFILE
  'data/raw/driving_dataset/Sensor_Dataset_features/2-Sensor_DataSet_features_part.csv'
INTO TABLE sensor_data_source
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(csv_index, booking_id, accuracy, bearing,
 acceleration_x, acceleration_y, acceleration_z,
 gyro_x, gyro_y, gyro_z, second, speed)
SET cleaned_date = NULL;

LOAD DATA LOCAL INFILE
  'data/raw/driving_dataset/Sensor_Dataset_features/3_Sensor_DataSet_features_part.csv'
INTO TABLE sensor_data_source
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(csv_index, booking_id, accuracy, bearing,
 acceleration_x, acceleration_y, acceleration_z,
 gyro_x, gyro_y, gyro_z, second, speed)
SET cleaned_date = NULL;

LOAD DATA LOCAL INFILE
  'data/raw/driving_dataset/Sensor_Dataset_features/4_Sensor_DataSet_features_part.csv'
INTO TABLE sensor_data_source
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(csv_index, booking_id, accuracy, bearing,
 acceleration_x, acceleration_y, acceleration_z,
 gyro_x, gyro_y, gyro_z, second, speed)
SET cleaned_date = NULL;

-- ---------- DRIVERS ----------
LOAD DATA LOCAL INFILE
  'data/raw/driving_dataset/drivers_dataset.csv'
INTO TABLE drivers_source
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(driver_id, driver_name, data_of_birth, No_of_Years_driving_exp,
 gender, car_brand, car_model_year, driver_rating)
SET cleaned_date = NULL;

-- ---------- SAFETY LABELS ----------
LOAD DATA LOCAL INFILE
  'data/raw/driving_dataset/safety_status_dataset.csv'
INTO TABLE safety_labels_source
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(booking_id, driver_id, label)
SET cleaned_date = NULL;

-- 7) QUICK SANITY COUNTS
SELECT COUNT(*) AS sensor_rows_raw   FROM sensor_data_source;
SELECT COUNT(*) AS driver_rows_raw   FROM drivers_source;
SELECT COUNT(*) AS safety_rows_raw   FROM safety_labels_source;

SELECT COUNT(*) AS sensor_rows_clean FROM sensor_data;
SELECT COUNT(*) AS driver_rows_clean FROM drivers;
SELECT COUNT(*) AS safety_rows_clean FROM safety_labels;

-- 8) Restore session defaults
SET unique_checks = 1;
SET foreign_key_checks = 1;
COMMIT;
SET autocommit = 1;
