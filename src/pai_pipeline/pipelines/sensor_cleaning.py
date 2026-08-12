#!/usr/bin/env python3

# -*- coding: utf-8 -*-

"""Run the PAI sensor ETL pipeline.

Auto-generated from notebook: PAI_Data_Cleaning_(Sensor) (3).ipynb

This script converts the notebook's executable code cells into a single Python
module with a clear entry point, adhering to PEP 8, PEP 257, and Google-style
docstrings where applicable.

Flow (depends on original notebook content):
    1) Extract raw/staging data.
    2) Clean & transform (impute, validate).
    3) Feature engineer trip-level metrics (FE_*).
    4) Load into the data warehouse tables.

Usage:
    python pai_sensor_pipeline.py

Generated: 2025-12-02T10:00:54
"""

# Note: label field from joined datasets is handled downstream when enriching sensor data.

import gc
import os
from typing import Dict, List

import mysql.connector
import numpy as np
import pandas as pd
from pyspark.ml.feature import Imputer, PCA, StandardScaler, VectorAssembler
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import types as T
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window

from pai_pipeline.sql import (
    quote_identifier,
    quote_qualified_identifier,
    sql_statement,
)


import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("pai_sensor_pipeline")


def run_pipeline():
    """Run the end-to-end pipeline extracted from the notebook.

    All code cells from the source notebook execute sequentially within
    this function scope to preserve order and variable sharing.
    """
    # === **Practical AI (ST1508) CA 1** ===\n# === 1. Libraries Import and Environment Setup ===\n# === 2. Data Extract Layer ===\n# === 2.1 Data Extraction From Staging Database ===\n# === 2.2 Data Quality Inspection ===\n# === 3 Data Transform Layer ===\n# === 3.1 Duplicate Handling ===\n# === 3.2 Data Types Casting ===\n# === 3.3 Removing Invalid Numerical Values ===\n# === 3.4 Missing Data Imputation ===\n# === 3.4.1 Data Preparation for Imputation ===\n# === 3.4.2 Machine Learning Imputation of Missing Data ===\n# === 3.5 Outliers Identification and Addressment ===\n# === 3.5.1 Univariate Outliers Detection ===\n# === 3.5.3 Multivariate Outlier Detection ===\n# === 3.6 Feature Engineering ===\n# === Row-level Signals ===\n# === Trip-level Aggregations ===\n# === 3.6.1 Setup, hygiene, and sorting ===\n# === 3.6.2 Row-level derived signals ===\n# === 3.6.3 Time-derivative and turning signals (Spark window-based) ===\n# === 3.6.4 Per-Trip Feature Builder ===\n# === Symbols (per trip) ===\n# === 1) Meta ===\n# === 2) Speed Behavior ===\n# === 3) Acceleration Magnitude & Axis Context ===\n# === 4) Jerk (abruptness from acceleration magnitude) ===\n# === 5) Gyroscope (rotational intensity) ===\n# === 6) Turning & Stability (bearing-based) ===\n# === 7) GPS Accuracy & Trip Edges ===\n# === Cell 4 — Per-trip aggregations in PySpark ===\n# === Cell 5 — Aggregate to per-trip table (with start/end speeds) ===\n# === 3.6.6 How These Features Help Detect Dangerous Driving ===\n# === 3.7 Data Audit ===\n# === 4. Data Load Layer ===

    # 1) Install MySQL Python client
    # !pip install -q mysql-connector-python

    # === S0: Setup — install MySQL connector + start Spark (Colab) ==============
    """
    Run this ONCE per Colab runtime.

    - Installs mysql-connector-python for talking to MySQL/RDS.
    - Starts a simple SparkSession (no JDBC jars needed).
    """

    # 2) Kill any old Spark session just in case
    active_spark = SparkSession.getActiveSession()
    if active_spark is not None:
        active_spark.stop()

    gc.collect()

    # 3) Start fresh Spark (Colab already has PySpark installed)
    spark = (
        SparkSession.builder.appName("PAI_CA1_Sensor_Cleaning")
        .master("local[*]")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )

    print("Spark started.")
    spark.range(5).show()

    # === S1: MySQL → pandas (typed) → Spark df_raw ==============================
    """
    Loads raw sensor_data_source rows from AWS RDS (where cleaned_date IS NULL),
    fixes dtypes in pandas, converts to a strongly-typed Spark DataFrame df_raw,
    then drops the pandas DataFrame to free RAM.

    Final schema (df_raw):
        csv_index      : string
        booking_id     : string
        accuracy       : double
        bearing        : double
        acceleration_x : double
        acceleration_y : double
        acceleration_z : double
        gyro_x         : double
        gyro_y         : double
        gyro_z         : double
        second         : double
        speed          : double
        cleaned_date   : string
    """

    # ---------- MySQL / RDS connection config ----------
    db_user = os.getenv("MYSQL_USER")
    db_pass = os.getenv("MYSQL_PASSWORD")
    db_host = os.getenv("MYSQL_STAGING_HOST")
    db_port = int(os.getenv("STAGING_DB_PORT"))
    db_name = os.getenv("STAGING_DB_NAME", "staging")
    db_table = os.getenv("STAGING_DB_TABLE", "sensor_data_source")
    quoted_db_table = quote_identifier(db_table)

    # ---------- SQL query ----------
    SELECT_COLS = [
        "csv_index",
        "booking_id",
        "accuracy",
        "bearing",
        "acceleration_x",
        "acceleration_y",
        "acceleration_z",
        "gyro_x",
        "gyro_y",
        "gyro_z",
        "`second`",
        "speed",
        "cleaned_date",
    ]
    select_list = ", ".join(SELECT_COLS)

    query = sql_statement(
        "SELECT",
        select_list,
        "FROM",
        quoted_db_table,
        "WHERE cleaned_date IS NULL",
    )

    print("Running SQL query against MySQL:\n", query)

    # ---------- Connect & load to pandas ----------
    cnx = mysql.connector.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_pass,
        database=db_name,
    )

    pdf = pd.read_sql(query, cnx)
    cnx.close()

    print("\n[MySQL → pandas] Shape:", pdf.shape)
    print(pdf.head())
    print("\n[MySQL → pandas] dtypes BEFORE fix:")
    print(pdf.dtypes)

    # ---------- Fix pandas dtypes explicitly ----------
    NUMERIC_COLS = [
        "accuracy",
        "bearing",
        "acceleration_x",
        "acceleration_y",
        "acceleration_z",
        "gyro_x",
        "gyro_y",
        "gyro_z",
        "second",
        "speed",
    ]

    # Make sure id-like columns stay as strings
    pdf["csv_index"] = pdf["csv_index"].astype(str)
    pdf["booking_id"] = pdf["booking_id"].astype(str)

    for c in NUMERIC_COLS:
        pdf[c] = pd.to_numeric(pdf[c], errors="coerce")

    # cleaned_date as string (even if all None for now)
    pdf["cleaned_date"] = pdf["cleaned_date"].astype(str)

    print("\n[MySQL → pandas] dtypes AFTER fix:")
    print(pdf.dtypes)

    # ---------- Define explicit Spark schema ----------
    schema = StructType(
        [
            StructField("csv_index", StringType(), True),
            StructField("booking_id", StringType(), True),
            StructField("accuracy", DoubleType(), True),
            StructField("bearing", DoubleType(), True),
            StructField("acceleration_x", DoubleType(), True),
            StructField("acceleration_y", DoubleType(), True),
            StructField("acceleration_z", DoubleType(), True),
            StructField("gyro_x", DoubleType(), True),
            StructField("gyro_y", DoubleType(), True),
            StructField("gyro_z", DoubleType(), True),
            StructField("second", DoubleType(), True),
            StructField("speed", DoubleType(), True),
            StructField("cleaned_date", StringType(), True),
        ]
    )

    # ---------- Convert pandas → Spark with schema ----------
    df_raw = spark.createDataFrame(pdf, schema=schema)

    print("\n[Spark] df_raw schema:")
    df_raw.printSchema()

    print("\n[Spark] Sample rows from df_raw:")
    df_raw.show(5, truncate=False)

    # ---------- Clean up pandas to free RAM ----------
    del pdf
    gc.collect()

    print("\n[CLEANUP] Dropped pandas DataFrame; df_raw is now the source of truth.")

    """
    Cell S2 — Baseline Audit (Spark, full df_raw)

    Uses df_raw from S1 (no extra MySQL hits).

    What this cell does:
      1) Uses the FULL df_raw as the audit frame.
      2) Computes missing values per column.
      3) Captures a data types snapshot from Spark.
      4) Computes duplicate summary (full-row equality).
    """

    print("Building audit view from FULL df_raw (no sampling) ...")

    # Use full df_raw for audit
    df_audit = df_raw.cache()
    total_rows = df_audit.count()
    cols = df_audit.columns

    print(f"Audit size: {total_rows:,} rows, {len(cols)} columns")

    # 1) Missing values per column (on full df_raw)
    missing_counts_df = df_audit.select(
        [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in cols]
    )

    missing_profile_df = (
        missing_counts_df.toPandas()
        .T.reset_index()
        .rename(columns={"index": "Column Name", 0: "Missing Count"})
    )

    missing_profile_df["Missing Percentage"] = (
        missing_profile_df["Missing Count"] / total_rows * 100
    ).round(2)

    # 2) dtypes snapshot (from Spark)
    dtypes_profile_df = spark.createDataFrame(
        [(c, str(t)) for c, t in df_audit.dtypes],
        schema=["Column Name", "Data Type"],
    ).toPandas()

    # 3) Duplicate summary on the full df_raw (full-row equality)
    dup_count = total_rows - df_audit.dropDuplicates().count()
    dup_percentage = round(dup_count / total_rows * 100, 4) if total_rows else 0.0

    dup_summary_pdf = spark.createDataFrame(
        [
            (
                total_rows,
                dup_count,
                dup_percentage,
                "Duplicates Found" if dup_count > 0 else "No Duplicates",
            )
        ],
        schema=[
            "Total Rows",
            "Duplicate Count",
            "Duplicate Percentage",
            "Status",
        ],
    ).toPandas()

    print(
        "Baseline Audit BEFORE Cleaning — "
        f"Rows: {total_rows:,}, Columns: {len(cols):,} "
        "(full df_raw in Spark)."
    )

    print(missing_profile_df.sort_values("Missing Percentage", ascending=False))
    print(dtypes_profile_df)
    print(dup_summary_pdf)

    # Optional: free cache if you don't need df_audit again
    df_audit.unpersist()

    """
    Cell S3 — Duplicate Handling (Spark, full-row duplicates ignoring csv_index)

    - Operates directly on df_raw (Spark DataFrame).
    - Treats rows as duplicates if ALL columns except `csv_index` are identical.
    - Keeps the first occurrence of each unique combination of
      (booking_id, accuracy, bearing, ..., speed, cleaned_date).
    - Preserves `csv_index` as a normal column (not part of the dedup key).
    - Returns df_dedup / df_clean_base for downstream cleaning steps.
    """

    # All columns in df_raw
    all_cols = df_raw.columns

    # Deduplication key = all columns EXCEPT csv_index
    dedup_key_cols = [c for c in all_cols if c != "csv_index"]

    print("Deduplication key columns (excluding csv_index):")
    print(dedup_key_cols)

    before = df_raw.count()

    df_dedup = df_raw.dropDuplicates(subset=dedup_key_cols)
    after = df_dedup.count()
    n_dups = before - after

    print(
        f"Dropped full-row duplicates ignoring `csv_index`: {n_dups:,} | "
        f"BEFORE — Rows: {before:,}, AFTER — Rows: {after:,}, Cols: {len(df_dedup.columns):,}"
    )

    # This becomes the base DF for subsequent cleaning steps
    df_clean_base = df_dedup

    """
    Cell S4 — Data Types: Fixed-Schema Casting Plan (PySpark)

    We enforce an explicit schema on df_dedup:

        • ID/Categorical:  booking_id  → string
        • Numeric:         accuracy, bearing,
                           acceleration_x, acceleration_y, acceleration_z,
                           gyro_x, gyro_y, gyro_z,
                           second, speed   → double

    Casting uses Spark's .cast("double") semantics:
        - invalid values become NULL (no hard failures)
        - we also report how many new NULLs are introduced per column.

    Inputs
    ------
    df_dedup : Spark DataFrame
        Output of S3 (after duplicate handling).

    Outputs
    -------
    df_cast : Spark DataFrame
        Typed version of df_dedup with enforced schema.
    """

    # ------------------- Config: target schema -------------------

    ID_COL = "booking_id"
    NUM_COLS: List[str] = [
        "accuracy",
        "bearing",
        "acceleration_x",
        "acceleration_y",
        "acceleration_z",
        "gyro_x",
        "gyro_y",
        "gyro_z",
        "second",
        "speed",
    ]

    # Only check columns that actually exist in df_dedup
    cols_to_check = [c for c in [ID_COL] + NUM_COLS if c in df_dedup.columns]

    # ------------------- 1) Null counts BEFORE casting -------------------

    before_nulls_row = df_dedup.select(
        [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in cols_to_check]
    ).collect()[0]

    before_nulls: Dict[str, int] = {c: int(before_nulls_row[c]) for c in cols_to_check}

    # Capture schema before casting
    schema_before = {name: dtype for name, dtype in df_dedup.dtypes}

    # ------------------- 2) Apply casts in Spark -------------------

    df_cast = df_dedup

    # ID as string (safe join key)
    if ID_COL in df_cast.columns:
        df_cast = df_cast.withColumn(ID_COL, F.col(ID_COL).cast("string"))

    # Numeric columns as double
    for col in NUM_COLS:
        if col in df_cast.columns:
            df_cast = df_cast.withColumn(col, F.col(col).cast("double"))

    # ------------------- 3) Null counts AFTER casting -------------------

    after_nulls_row = df_cast.select(
        [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in cols_to_check]
    ).collect()[0]

    after_nulls: Dict[str, int] = {c: int(after_nulls_row[c]) for c in cols_to_check}

    # Capture schema after casting
    schema_after = {name: dtype for name, dtype in df_cast.dtypes}

    # ------------------- 4) Build casting report (small, via pandas) -------------------

    report_rows: List[Dict[str, object]] = []
    for c in cols_to_check:
        report_rows.append(
            {
                "Column": c,
                "From Type": schema_before.get(c, "N/A"),
                "To Type": schema_after.get(c, "N/A"),
                "New NULLs Added": after_nulls[c] - before_nulls[c],
                "NULLs Before": before_nulls[c],
                "NULLs After": after_nulls[c],
            }
        )

    dtype_cast_report = pd.DataFrame(report_rows)

    print("Post-cast Spark dtypes:")
    dtypes_df = pd.DataFrame(df_cast.dtypes, columns=["Column Name", "Data Type"])
    print(dtypes_df)

    print("\nCasting report:")
    print(dtype_cast_report)

    # Optional: make this the main working DF for next cells
    df_typed = df_cast

    """
    Cell C6 — Rule-Based Validation (Accuracy, Bearing, Speed, Second) — Spark

    Business rules:
        - Invalid Accuracy (< 0)             → set to NULL
        - Invalid Bearing (outside [0, 360]) → set to NULL
        - Invalid Speed (< 0)                → set to NULL
        - Invalid Second (< 0)               → set to NULL

    Also:
        - Adds temporary flag columns:
            * accuracy_invalid (0/1)
            * bearing_invalid  (0/1)
            * speed_invalid    (0/1)
            * second_invalid   (0/1)
            * row_any_invalid  (0/1 if any of the above is 1)
        - Prints a summary of how many values/rows were flagged.
        - Drops the helper flag columns at the end.

    Inputs
    ------
    df_typed : Spark DataFrame
        Output of S4 (typed schema: numeric cols as double).

    Outputs
    -------
    df_validated : Spark DataFrame
        Same columns as df_typed, but with invalid values set to NULL.
    """

    EPS = 1e-12  # small tolerance for floating-point comparisons

    # Start from the typed DF (from S4)
    df_validated = df_typed

    # Ensure numeric types (defensive; S4 already cast them)
    for c in ["accuracy", "bearing", "speed", "second"]:
        if c in df_validated.columns:
            df_validated = df_validated.withColumn(c, F.col(c).cast("double"))

    # ---- Accuracy invalid (< 0) -----------------------------------------------
    if "accuracy" in df_validated.columns:
        df_validated = df_validated.withColumn(
            "accuracy_invalid",
            F.when(
                (F.col("accuracy").isNotNull()) & (F.col("accuracy") < -EPS),
                F.lit(1),
            ).otherwise(F.lit(0)),
        ).withColumn(
            "accuracy",
            F.when(F.col("accuracy_invalid") == 1, F.lit(None)).otherwise(
                F.col("accuracy")
            ),
        )
    else:
        df_validated = df_validated.withColumn("accuracy_invalid", F.lit(0))

    # ---- Bearing invalid (not in [0, 360]) ------------------------------------
    if "bearing" in df_validated.columns:
        df_validated = df_validated.withColumn(
            "bearing_invalid",
            F.when(
                (F.col("bearing").isNotNull())
                & ((F.col("bearing") < 0 - EPS) | (F.col("bearing") > 360 + EPS)),
                F.lit(1),
            ).otherwise(F.lit(0)),
        ).withColumn(
            "bearing",
            F.when(F.col("bearing_invalid") == 1, F.lit(None)).otherwise(
                F.col("bearing")
            ),
        )
    else:
        df_validated = df_validated.withColumn("bearing_invalid", F.lit(0))

    # ---- Speed invalid (< 0) --------------------------------------------------
    if "speed" in df_validated.columns:
        df_validated = df_validated.withColumn(
            "speed_invalid",
            F.when(
                (F.col("speed").isNotNull()) & (F.col("speed") < -EPS),
                F.lit(1),
            ).otherwise(F.lit(0)),
        ).withColumn(
            "speed",
            F.when(F.col("speed_invalid") == 1, F.lit(None)).otherwise(F.col("speed")),
        )
    else:
        df_validated = df_validated.withColumn("speed_invalid", F.lit(0))

    # ---- Second invalid (< 0) -------------------------------------------------
    if "second" in df_validated.columns:
        df_validated = df_validated.withColumn(
            "second_invalid",
            F.when(
                (F.col("second").isNotNull()) & (F.col("second") < -EPS),
                F.lit(1),
            ).otherwise(F.lit(0)),
        ).withColumn(
            "second",
            F.when(F.col("second_invalid") == 1, F.lit(None)).otherwise(
                F.col("second")
            ),
        )
    else:
        df_validated = df_validated.withColumn("second_invalid", F.lit(0))

    # ---- Row-level invalid helper ---------------------------------------------
    df_validated = df_validated.withColumn(
        "row_any_invalid",
        (
            F.col("accuracy_invalid")
            + F.col("bearing_invalid")
            + F.col("speed_invalid")
            + F.col("second_invalid")
            > 0
        ).cast("int"),
    )

    # ---- Summaries ------------------------------------------------------------
    summary_pdf = df_validated.agg(
        F.sum("accuracy_invalid").alias("invalid_accuracy_count"),
        F.sum("bearing_invalid").alias("invalid_bearing_count"),
        F.sum("speed_invalid").alias("invalid_speed_count"),
        F.sum("second_invalid").alias("invalid_second_count"),
        F.sum("row_any_invalid").alias("invalid_rows_any"),
        F.count(F.lit(1)).alias("total_rows"),
    ).toPandas()

    print("Validation summary (accuracy / bearing / speed / second rules):")
    print(summary_pdf)

    # ---- Optional cleanup: drop helper columns --------------------------------
    df_validated = df_validated.drop(
        "accuracy_invalid",
        "bearing_invalid",
        "speed_invalid",
        "second_invalid",
        "row_any_invalid",
    )

    """
    Cell C5 — NA Pruning Before Imputation (Spark)

    What this does (on df_validated):
      1) Drop 'cleaned_date' (we don't impute it).
      2) Drop rows where ALL numeric cols are NULL.
      3) Drop numeric columns that are entirely NULL (no observed non-null values).

    Inputs
    ------
    df_validated : Spark DataFrame
        Output of C6 (after rule-based validation).

    Outputs
    -------
    df_prep : Spark DataFrame
        Pruned DataFrame ready for imputation.
    """

    numeric_cols = [
        "accuracy",
        "bearing",
        "acceleration_x",
        "acceleration_y",
        "acceleration_z",
        "gyro_x",
        "gyro_y",
        "gyro_z",
        "second",
        "speed",
    ]

    df_prep = df_validated

    # 0) Drop cleaned_date (we don't impute it)
    if "cleaned_date" in df_prep.columns:
        df_prep = df_prep.drop("cleaned_date")
        print("Dropped 'cleaned_date' before imputation.")

    # 1) Drop all-NULL rows across numeric columns
    null_exprs = [F.col(c).isNull() for c in numeric_cols if c in df_prep.columns]

    if null_exprs:
        all_null_cond = null_exprs[0]
        for expr in null_exprs[1:]:
            all_null_cond = all_null_cond & expr

        before_rows = df_prep.count()
        df_prep = df_prep.filter(~all_null_cond)
        after_rows = df_prep.count()
        print("Dropped all-NULL numeric rows:", before_rows - after_rows)
    else:
        print("No numeric columns found for all-NULL row pruning.")

    # 2) Drop numeric columns that are entirely NULL
    cols_present = df_prep.columns
    numeric_present = [c for c in numeric_cols if c in cols_present]

    to_drop = []
    for c in numeric_present:
        non_null = df_prep.filter(F.col(c).isNotNull()).limit(1).count()
        if non_null == 0:
            to_drop.append(c)

    if to_drop:
        df_prep = df_prep.drop(*to_drop)
        print("Dropped numeric cols with zero observed values:", to_drop)
    else:
        print("No numeric columns were entirely NULL.")

    print("After pruning:", df_prep.count(), "rows,", len(df_prep.columns), "cols")

    """
    Cell C6 — Missing Data Imputation via Spark ML Imputer (Median)

    - Operates on df_prep (Spark DataFrame from C5).
    - Uses pyspark.ml.feature.Imputer to fill numeric columns with the median.
    - booking_id is left untouched, even if it has a numeric dtype.
    - Output: df_imputed (Spark DataFrame) with the same column names as df_prep.
    """

    # 1) Detect numeric columns eligible for imputation
    numeric_present = [
        c
        for c, t in df_prep.dtypes
        if t in ("double", "float", "int", "bigint") and c != "booking_id"
    ]

    print("Numeric columns to impute:", numeric_present)

    if numeric_present:
        # 2) Configure Spark ML Imputer (median strategy)
        imputer = Imputer(
            inputCols=numeric_present,
            outputCols=[f"{c}__imputed" for c in numeric_present],
            strategy="median",
        )

        # 3) Fit on df_prep and transform
        model = imputer.fit(df_prep)
        df_imputed_tmp = model.transform(df_prep)

        # 4) Replace original numeric columns with their imputed versions
        for c in numeric_present:
            df_imputed_tmp = df_imputed_tmp.drop(c).withColumnRenamed(
                f"{c}__imputed", c
            )
    else:
        print("No numeric columns detected for imputation. Skipping Imputer step.")
        df_imputed_tmp = df_prep

    # 5) Final imputed DF
    df_imputed = df_imputed_tmp

    # 6) Quick sanity check: remaining NULLs per column
    print("Imputation complete. Remaining NULLs per column:")
    df_imputed.select(
        [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in df_imputed.columns]
    ).show(truncate=False)

    """
    Cell C7 — Univariate Outlier Flagging (Spark, IQR)

    - For each numeric column, compute IQR and flag points outside:
          [Q1 - 1.5 * IQR, Q3 + 1.5 * IQR]
    - Adds boolean columns: "<col>__is_uni_outlier"

    Inputs
    ------
    df_imputed : Spark DataFrame
        Output of C6 (after median imputation).

    Outputs
    -------
    df_uni_flagged : Spark DataFrame
        df_imputed + one boolean flag column per numeric feature.
    """

    # 1) Identify numeric columns (exclude booking_id)
    numeric_for_uni = [
        c
        for c, t in df_imputed.dtypes
        if t in ("double", "float", "int", "bigint") and c != "booking_id"
    ]

    print("Numeric columns for univariate IQR outlier check:", numeric_for_uni)

    df_uni = df_imputed
    total_rows = df_uni.count()

    uni_summary_rows = []

    # 2) Loop through each numeric column and apply IQR rule
    for c in numeric_for_uni:
        # approxQuantile returns [q1, q3]
        q1, q3 = df_uni.approxQuantile(c, [0.25, 0.75], 0.001)
        iqr = q3 - q1

        if iqr == 0:
            # No variability => no outliers
            df_uni = df_uni.withColumn(f"{c}__is_uni_outlier", F.lit(False))
            uni_summary_rows.append((c, "iqr", 0, 0.0))
            continue

        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        flag_col_name = f"{c}__is_uni_outlier"
        flag_expr = F.when(
            (F.col(c) < lower) | (F.col(c) > upper), F.lit(True)
        ).otherwise(F.lit(False))

        df_uni = df_uni.withColumn(flag_col_name, flag_expr)

        n_out = df_uni.filter(F.col(flag_col_name) == True).count()
        pct_out = (n_out / total_rows * 100.0) if total_rows else 0.0

        uni_summary_rows.append((c, "iqr", n_out, pct_out))

    # 3) Build summary table (Spark → small pandas for display)
    uni_summary_spark = spark.createDataFrame(
        uni_summary_rows,
        schema=["column", "method", "n_outliers", "pct_outliers"],
    )

    uni_summary = uni_summary_spark.orderBy(F.desc("pct_outliers")).toPandas()

    print("Univariate outliers flagged (IQR).")
    print(uni_summary.head(12))

    # 4) Hand-off
    df_uni_flagged = df_uni

    """
    Cell C8 — Multivariate Outlier Flagging (Spark, PCA Reconstruction)

    Approach
    --------
    1) Use numeric features (excluding booking_id) to build a feature vector.
    2) Standardize features (zero mean, unit variance) using StandardScaler.
    3) Fit PCA with k components (k <= 3, capped by number of features).
    4) Compute reconstruction error in scaled space:
           error = || x_scaled - x_hat ||^2
           where x_hat = PC * z,  z = PCA projection.
    5) Normalise error to [0, 1] as `_mv_score`.
    6) Flag top X% (contamination) as multivariate outliers:
           `_mv_is_outlier` = True/False.

    Inputs
    ------
    df_uni_flagged : Spark DataFrame
        Output from C7 with univariate outlier flags.

    Outputs
    -------
    df_clean : Spark DataFrame
        df_uni_flagged + `_mv_score` + `_mv_is_outlier`
    """

    # 0) Select numeric columns for multivariate detection
    numeric_mv = [
        c
        for c, t in df_uni_flagged.dtypes
        if t in ("double", "float", "int", "bigint") and c != "booking_id"
    ]

    print("Numeric columns for MV PCA detection:", numeric_mv)

    if not numeric_mv:
        # No numeric features → nothing to do
        df_clean = df_uni_flagged.withColumn("_mv_score", F.lit(0.0)).withColumn(
            "_mv_is_outlier", F.lit(False)
        )
        print(
            "No numeric columns for MV detection; `_mv_score` set to 0, no MV outliers flagged."
        )
    else:
        # 1) Build feature vector
        assembler = VectorAssembler(
            inputCols=numeric_mv,
            outputCol="features",
            handleInvalid="skip",  # rows with invalids get skipped from vectorisation
        )
        df_vec = assembler.transform(df_uni_flagged)

        # 2) Standardise features
        scaler = StandardScaler(
            inputCol="features",
            outputCol="features_scaled",
            withMean=True,
            withStd=True,
        )
        scaler_model = scaler.fit(df_vec)
        df_scaled = scaler_model.transform(df_vec)

        # 3) PCA in scaled feature space
        d = len(numeric_mv)
        k = min(d, 3) if d > 0 else 0

        if k == 0:
            # Degenerate case: only 1 numeric col or similar
            df_tmp = df_scaled.withColumn("_mv_score", F.lit(0.0)).withColumn(
                "_mv_is_outlier", F.lit(False)
            )
            # Clean temp vector columns if they exist
            for tmp_col in ["features", "features_scaled"]:
                if tmp_col in df_tmp.columns:
                    df_tmp = df_tmp.drop(tmp_col)
            df_clean = df_tmp
            print("k=0 for PCA; `_mv_score` set to 0, no MV outliers flagged.")
        else:
            pca = PCA(
                k=k,
                inputCol="features_scaled",
                outputCol="pca_features",
            )
            pca_model = pca.fit(df_scaled)

            # This was missing: actually create `pca_features`
            df_pca = pca_model.transform(df_scaled)

            pc = pca_model.pc  # DenseMatrix of shape (d, k)

            # Broadcast PC to executors
            pc_bc = spark.sparkContext.broadcast(pc)

            @F.udf(T.DoubleType())
            def recon_error(x_scaled, z):
                """Compute squared PCA reconstruction error in scaled feature space.

                Args:
                    x_scaled: Scaled sensor feature vector.
                    z: PCA-coordinate vector for the same observation.

                Returns:
                    Squared reconstruction error, or ``None`` for missing vectors.
                """
                if x_scaled is None or z is None:
                    return None

                pc_local = pc_bc.value  # DenseMatrix
                d_local, k_local = pc_local.numRows, pc_local.numCols

                # Convert Spark vectors to numpy arrays
                try:
                    x_arr = np.array(x_scaled.toArray(), dtype=float)
                    z_arr = np.array(z.toArray(), dtype=float)
                except AttributeError:
                    # Fallback if already arrays/list-like
                    x_arr = np.array(x_scaled, dtype=float)
                    z_arr = np.array(z, dtype=float)

                # Guard shapes
                if x_arr.shape[0] != d_local or z_arr.shape[0] != k_local:
                    return None

                # x_hat = PC * z
                x_hat = np.zeros(d_local, dtype=float)
                for col_idx in range(k_local):
                    zval = float(z_arr[col_idx])
                    if zval == 0.0:
                        continue
                    for row_idx in range(d_local):
                        x_hat[row_idx] += pc_local[row_idx, col_idx] * zval

                diff = x_arr - x_hat
                return float(np.dot(diff, diff))

            # 4) Score each row with reconstruction error
            df_scored = df_pca.withColumn(
                "_mv_score_raw",
                recon_error(F.col("features_scaled"), F.col("pca_features")),
            )

            # 5) Normalise scores to [0, 1]
            stats_row = df_scored.agg(
                F.min("_mv_score_raw").alias("min_s"),
                F.max("_mv_score_raw").alias("max_s"),
            ).collect()[0]

            min_s = stats_row["min_s"]
            max_s = stats_row["max_s"]

            if min_s is None or max_s is None:
                # All scores NULL? Fallback: no MV signal
                df_tmp = df_scored.withColumn("_mv_score", F.lit(0.0)).withColumn(
                    "_mv_is_outlier", F.lit(False)
                )
                for tmp_col in [
                    "features",
                    "features_scaled",
                    "pca_features",
                    "_mv_score_raw",
                ]:
                    if tmp_col in df_tmp.columns:
                        df_tmp = df_tmp.drop(tmp_col)
                df_clean = df_tmp
                print("All MV scores are NULL; treating as no MV outliers.")
            else:
                denom = (max_s - min_s) if max_s != min_s else 1e-12
                df_scored = df_scored.withColumn(
                    "_mv_score",
                    (F.col("_mv_score_raw") - F.lit(min_s)) / F.lit(denom),
                )

                # 6) Determine contamination rate and threshold
                total_mv = df_scored.count()
                if total_mv < 500:
                    contamination = 0.05
                elif total_mv < 5000:
                    contamination = 0.02
                else:
                    contamination = 0.01

                thr = df_scored.approxQuantile(
                    "_mv_score_raw",
                    [1.0 - contamination],
                    0.001,
                )[0]

                # 7) Flag MV outliers and clean temp columns
                df_tmp = df_scored.withColumn(
                    "_mv_is_outlier",
                    (F.col("_mv_score_raw") >= F.lit(thr)).cast("boolean"),
                )

                for tmp_col in [
                    "features",
                    "features_scaled",
                    "pca_features",
                    "_mv_score_raw",
                ]:
                    if tmp_col in df_tmp.columns:
                        df_tmp = df_tmp.drop(tmp_col)

                df_clean = df_tmp

                print("MV detection via PCA reconstruction")
                print("Contamination:", contamination, "| threshold_raw:", thr)
                print(
                    "MV outliers flagged:",
                    df_clean.filter(F.col("_mv_is_outlier") == True).count(),
                )

    """
    Cell 1 — Setup & Hygiene (PySpark)

    Purpose
    -------
    Create a clean working copy `dfc` from df_clean, with:
        - Numeric sensor columns cast to double.
        - booking_id cast to string.
        - Optional ordering by booking_id, second for inspection.

    Inputs
    ------
    df_clean : Spark DataFrame
        Output of C8 (after all cleaning & outlier flagging).

    Outputs
    -------
    dfc : Spark DataFrame
        Sanitised working DataFrame for downstream feature engineering.
    """

    # Start from the cleaned DF from previous step
    dfc = df_clean

    # Numeric columns we expect (cast defensively)
    _NUM_COLS = [
        "accuracy",
        "bearing",
        "acceleration_x",
        "acceleration_y",
        "acceleration_z",
        "gyro_x",
        "gyro_y",
        "gyro_z",
        "second",
        "speed",
    ]

    # Ensure numeric columns are double where present
    for col in _NUM_COLS:
        if col in dfc.columns:
            dfc = dfc.withColumn(col, F.col(col).cast("double"))

    # Ensure booking_id exists and is consistently string
    if "booking_id" in dfc.columns:
        dfc = dfc.withColumn("booking_id", F.col("booking_id").cast("string"))

    # Optional: create a globally sorted view for inspection / later window ops
    _sort_cols = [c for c in ("booking_id", "second") if c in dfc.columns]
    if _sort_cols:
        dfc = dfc.orderBy(*_sort_cols)

    print("Cell 1 complete — dfc schema:")
    dfc.printSchema()

    # ---------------- Magnitudes ----------------

    dfc = dfc.withColumn(
        "acc_magnitude",
        F.sqrt(
            F.col("acceleration_x") ** 2
            + F.col("acceleration_y") ** 2
            + F.col("acceleration_z") ** 2
        ),
    )

    dfc = dfc.withColumn(
        "gyro_magnitude",
        F.sqrt(F.col("gyro_x") ** 2 + F.col("gyro_y") ** 2 + F.col("gyro_z") ** 2),
    )

    print("Cell F1 complete — added acc_magnitude and gyro_magnitude.")
    dfc.select("booking_id", "second", "acc_magnitude", "gyro_magnitude").show(
        10, truncate=False
    )

    """
    Cell F2 — Time-Derivative & Turning Signals (PySpark)

    Purpose
    -------
    Per booking_id (trip), compute:
        - delta_speed       = v_t - v_{t-1}
        - delta_t           = t - t_{t-1}
        - speed_change_rate = |delta_speed| / delta_t
        - delta_accm        = acc_magnitude_t - acc_magnitude_{t-1}
        - jerk              = |delta_accm| / delta_t
        - bearing_diff_deg  = minimal heading change in degrees (0–180)
        - turn_rate_deg_s   = bearing_diff_deg / delta_t
    """

    w_trip_time = Window.partitionBy("booking_id").orderBy("second")

    # Δspeed and Δt
    dfc = dfc.withColumn(
        "delta_speed", F.col("speed") - F.lag("speed").over(w_trip_time)
    ).withColumn("delta_t", F.col("second") - F.lag("second").over(w_trip_time))

    # Speed change rate |Δv| / Δt
    dfc = dfc.withColumn(
        "speed_change_rate",
        F.when(F.col("delta_t") > 0, F.abs(F.col("delta_speed") / F.col("delta_t"))),
    )

    # Jerk from acceleration magnitude: |Δacc_magnitude| / Δt
    dfc = dfc.withColumn(
        "delta_accm", F.col("acc_magnitude") - F.lag("acc_magnitude").over(w_trip_time)
    ).withColumn(
        "jerk",
        F.when(F.col("delta_t") > 0, F.abs(F.col("delta_accm") / F.col("delta_t"))),
    )

    # Bearing-based turning:
    # Use minimal angle difference to approximate unwrapped heading changes.
    dfc = dfc.withColumn(
        "bearing_diff_raw", F.abs(F.col("bearing") - F.lag("bearing").over(w_trip_time))
    ).withColumn(
        "bearing_diff_deg",
        F.when(
            F.col("bearing_diff_raw") > 180, 360 - F.col("bearing_diff_raw")
        ).otherwise(F.col("bearing_diff_raw")),
    )

    dfc = dfc.withColumn(
        "turn_rate_deg_s",
        F.when(F.col("delta_t") > 0, F.col("bearing_diff_deg") / F.col("delta_t")),
    )

    print("Cell F2 complete — added time-derivative and turning signals.")
    dfc.select(
        "booking_id",
        "second",
        "speed",
        "delta_speed",
        "delta_t",
        "speed_change_rate",
        "jerk",
        "bearing_diff_deg",
        "turn_rate_deg_s",
    ).show(10, truncate=False)

    """
    Cell F3 — Per-Trip Aggregations (PySpark)

    Purpose
    -------
    Aggregate second-level sensor features into trip-level features (one row per booking_id).

    For each booking_id, compute:
        - FE_n_samples           : number of sensor rows in the trip
        - FE_trip_duration_s     : max(second) - min(second)
        - FE_sampling_rate_hz    : FE_n_samples / FE_trip_duration_s

    Speed behaviour:
        - FE_speed_mean, FE_speed_std, FE_speed_max, FE_speed_min
        - FE_speed_p90           : 90th percentile of speed
        - FE_speed_change_rate_med : median(|Δv| / Δt)

    Acceleration magnitude:
        - FE_acc_mag_mean, FE_acc_mag_std, FE_acc_mag_max, FE_acc_mag_p95

    Axis-level acceleration:
        - FE_acc_x_mean, FE_acc_y_mean, FE_acc_z_mean
        - FE_acc_x_std,  FE_acc_y_std,  FE_acc_z_std

    Jerk:
        - FE_jerk_med  : median jerk
        - FE_jerk_p95  : 95th percentile of jerk

    Gyro magnitude:
        - FE_gyro_mag_mean, FE_gyro_mag_std, FE_gyro_mag_p95

    Turning:
        - FE_turn_delta_std_deg : std-dev of bearing_diff_deg
        - FE_turn_rate_med_deg_s: median turn_rate_deg_s

    GPS accuracy:
        - FE_gps_acc_mean, FE_gps_acc_std
    """

    # Core per-trip aggregates (1 row per booking_id)
    trip_aggs = dfc.groupBy("booking_id").agg(
        # Meta
        F.count(F.lit(1)).alias("FE_n_samples"),
        (F.max("second") - F.min("second")).alias("FE_trip_duration_s"),
        # Speed behaviour
        F.avg("speed").alias("FE_speed_mean"),
        F.stddev_pop("speed").alias("FE_speed_std"),
        F.max("speed").alias("FE_speed_max"),
        F.min("speed").alias("FE_speed_min"),
        F.expr("percentile_approx(speed, 0.90)").alias("FE_speed_p90"),
        F.expr("percentile_approx(speed_change_rate, 0.50)").alias(
            "FE_speed_change_rate_med"
        ),
        # Acceleration magnitude
        F.avg("acc_magnitude").alias("FE_acc_mag_mean"),
        F.stddev_pop("acc_magnitude").alias("FE_acc_mag_std"),
        F.max("acc_magnitude").alias("FE_acc_mag_max"),
        F.expr("percentile_approx(acc_magnitude, 0.95)").alias("FE_acc_mag_p95"),
        # Axis-level acceleration
        F.avg("acceleration_x").alias("FE_acc_x_mean"),
        F.avg("acceleration_y").alias("FE_acc_y_mean"),
        F.avg("acceleration_z").alias("FE_acc_z_mean"),
        F.stddev_pop("acceleration_x").alias("FE_acc_x_std"),
        F.stddev_pop("acceleration_y").alias("FE_acc_y_std"),
        F.stddev_pop("acceleration_z").alias("FE_acc_z_std"),
        # Jerk
        F.expr("percentile_approx(jerk, 0.50)").alias("FE_jerk_med"),
        F.expr("percentile_approx(jerk, 0.95)").alias("FE_jerk_p95"),
        # Gyro magnitude
        F.avg("gyro_magnitude").alias("FE_gyro_mag_mean"),
        F.stddev_pop("gyro_magnitude").alias("FE_gyro_mag_std"),
        F.expr("percentile_approx(gyro_magnitude, 0.95)").alias("FE_gyro_mag_p95"),
        # Turning
        F.stddev_pop("bearing_diff_deg").alias("FE_turn_delta_std_deg"),
        F.expr("percentile_approx(turn_rate_deg_s, 0.50)").alias(
            "FE_turn_rate_med_deg_s"
        ),
        # GPS accuracy
        F.avg("accuracy").alias("FE_gps_acc_mean"),
        F.stddev_pop("accuracy").alias("FE_gps_acc_std"),
    )

    # Sampling rate (Hz) = n_samples / duration (only if duration > 0)
    trip_features = trip_aggs.withColumn(
        "FE_sampling_rate_hz",
        F.when(
            F.col("FE_trip_duration_s") > 0,
            F.col("FE_n_samples") / F.col("FE_trip_duration_s"),
        ),
    )

    print("Cell F3 complete — core FE_ features computed per booking_id.")
    trip_features.select(
        "booking_id",
        "FE_n_samples",
        "FE_trip_duration_s",
        "FE_sampling_rate_hz",
        "FE_speed_mean",
        "FE_acc_mag_mean",
    ).show(10, truncate=False)

    """
    Cell F4 — Start/End Speed & Final Trip Feature Table (PySpark)

    Purpose
    -------
    For each booking_id (trip), add:
        - FE_start_speed : speed at earliest timestamp (second)
        - FE_end_speed   : speed at latest timestamp (second)

    Then merge these into `trip_features` to produce a final
    per-trip feature table ready for downstream ML / DW loading.
    """

    # Windows for first and last sample per trip
    w_first = Window.partitionBy("booking_id").orderBy("second")
    w_last = Window.partitionBy("booking_id").orderBy(F.col("second").desc())

    # Attach row numbers for first/last detection
    df_start_end = dfc.withColumn("rn_first", F.row_number().over(w_first)).withColumn(
        "rn_last", F.row_number().over(w_last)
    )

    # Start speed per trip (earliest second)
    start_speed_df = df_start_end.filter(F.col("rn_first") == 1).select(
        "booking_id", F.col("speed").alias("FE_start_speed")
    )

    # End speed per trip (latest second)
    end_speed_df = df_start_end.filter(F.col("rn_last") == 1).select(
        "booking_id", F.col("speed").alias("FE_end_speed")
    )

    # Join onto existing trip-level features
    trip_features = trip_features.join(
        start_speed_df, on="booking_id", how="left"
    ).join(end_speed_df, on="booking_id", how="left")

    print("Cell F4 complete — final trip-level feature table ready.")
    trip_features.show(10, truncate=False)

    """
    Cell C9 — AFTER Audit & Optional Persist (Spark Version)

    Purpose
    -------
    Compute AFTER-cleaning audit metrics for the final cleaned Spark
    DataFrame (df_clean), including:
        • Shape (n_rows, n_cols)
        • Missing values per column
        • Dtype snapshot
        • Duplicate count (full-row)

    Optionally: persist df_clean to Parquet/CSV/MySQL DW.

    Notes
    -----
    - df_clean is the final cleaned Spark DataFrame
      (output from C8 after MV outlier scoring/filtering).
    - All heavy computations are Spark-native.
    - Pandas is only used for small display tables.
    """

    # ---------------------------------------------------------------------
    # 0. Sanity check
    # ---------------------------------------------------------------------
    print("[C9] Running AFTER-audit on df_clean")
    print("Columns in df_clean:", df_clean.columns)

    # ---------------------------------------------------------------------
    # 1. Shape
    # ---------------------------------------------------------------------
    after_rows = df_clean.count()
    after_cols = len(df_clean.columns)

    print(f"[AFTER] Shape = ({after_rows:,}, {after_cols})")

    # ---------------------------------------------------------------------
    # 2. Missing values per column (Spark → small pandas for display)
    # ---------------------------------------------------------------------
    missing_df = df_clean.select(
        [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in df_clean.columns]
    )

    missing_pdf = (
        missing_df.toPandas()
        .T.reset_index()
        .rename(columns={"index": "Column", 0: "Missing Count"})
    )
    missing_pdf["Missing %"] = (missing_pdf["Missing Count"] / after_rows * 100).round(
        2
    )

    print("\n[AFTER] Missing value summary:")
    print(missing_pdf.head(12))

    # ---------------------------------------------------------------------
    # 3. Dtype snapshot
    # ---------------------------------------------------------------------
    dtype_pdf = pd.DataFrame(df_clean.dtypes, columns=["Column", "Spark DataType"])

    print("\n[AFTER] Dtype snapshot:")
    print(dtype_pdf)

    # ---------------------------------------------------------------------
    # 4. Duplicate count (full-row equality)
    # ---------------------------------------------------------------------
    after_dups = after_rows - df_clean.dropDuplicates().count()

    dup_summary_pdf = pd.DataFrame(
        [
            {
                "Total Rows": after_rows,
                "Duplicate Count": after_dups,
                "Duplicate %": round(after_dups / after_rows * 100, 5)
                if after_rows
                else 0,
                "Status": "Duplicates Found" if after_dups > 0 else "No Duplicates",
            }
        ]
    )

    print("\n[AFTER] Duplicate summary:")
    print(dup_summary_pdf)

    # ---------------------------------------------------------------------
    # 5. (Optional) Persist Cleaned Data — choose one
    # ---------------------------------------------------------------------
    # Options:
    #   None      → no write
    #   "parquet" → write to Parquet folder
    #   "csv"     → write to CSV folder
    #   "mysql"   → write to MySQL DW using JDBC (requires MySQL JAR on Spark classpath)
    # ---------------------------------------------------------------------

    SAVE_TO = None  # "parquet", "csv", or "mysql"
    SAVE_PATH = "/content/df_clean"  # local path (Colab). Adjust if needed.

    if SAVE_TO == "parquet":
        (df_clean.write.mode("overwrite").parquet(SAVE_PATH))
        print(f"\n[WRITE] Saved cleaned dataset to Parquet → {SAVE_PATH}")

    elif SAVE_TO == "csv":
        (df_clean.write.mode("overwrite").option("header", True).csv(SAVE_PATH))
        print(f"\n[WRITE] Saved cleaned dataset to CSV folder → {SAVE_PATH}")

    elif SAVE_TO == "mysql":
        # --- DW / warehouse config (same RDS host, different schema) ---
        dw_host = os.getenv("DW_DB_HOST")
        dw_port = int(os.getenv("DW_DB_PORT", "3306"))
        dw_name = os.getenv("DW_DB_NAME", "warehouse")  # your DW schema
        dw_user = os.getenv("DW_DB_USER")
        dw_pass = os.getenv("DW_DB_PASSWORD")

        missing_dw_vars = [
            name
            for name, value in {
                "DW_DB_HOST": dw_host,
                "DW_DB_USER": dw_user,
                "DW_DB_PASSWORD": dw_pass,
            }.items()
            if not value
        ]
        if missing_dw_vars:
            raise ValueError(
                "SAVE_TO='mysql' requires DW database env vars: "
                + ", ".join(missing_dw_vars)
            )

        # Final fact/staging table name in DW
        dw_table = os.getenv("DW_DB_TABLE", "Fact_Cleaned_Sensor")

        jdbc_url = (
            f"jdbc:mysql://{dw_host}:{dw_port}/{dw_name}"
            "?serverTimezone=UTC&characterEncoding=UTF-8"
        )

        print("\n[WRITE] Writing df_clean to MySQL DW ...")
        print("  URL   :", jdbc_url)
        print("  Table :", dw_table)

        (
            df_clean.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", dw_table)
            .option("user", dw_user)
            .option("password", dw_pass)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .mode("append")
            .save()
        )

        print("[WRITE] df_clean successfully appended to MySQL DW.")

    else:
        print("\n[WRITE] SAVE_TO is None — no persistence performed in C9.")

    """
    Cell C11 — Load trip_features into Dim_Sensor (streaming) + Mark Staging

    Updated version (fix):
    - Uses df_clean as the source for raw sensor aggregates instead of df_typed.
    - Ensures the final DataFrame has BOTH:
        • Aggregated raw columns: accuracy, bearing, accel_*, gyro_*, second, speed
        • Feature-engineered columns: FE_*
    - Aligns the output to the Dim_Sensor schema and loads to MySQL (warehouse.Dim_Sensor).
    - Marks the corresponding staging.sensor_data_source rows as cleaned.

    Grain
    -----
    1 row per booking_id (trip-level profile).

    Inputs (globals)
    ----------------
    df_clean : Spark DataFrame
        Output of C8 — fully cleaned row-level sensor data
        (includes original columns + outlier flags, etc.).

    trip_features : Spark DataFrame
        Output of F3 — per-trip aggregation of FE_* features.

    db_user, db_pass, db_host, db_port, db_name, db_table
        Connection info for the STAGING database (sensor_data_source table).

    Environment Variables (optional overrides)
    ------------------------------------------
    DW_USER, DW_PASS, DW_HOST, DW_PORT, DW_DB, DW_TABLE
        If set, override the default DW connection and table.
    """

    # ---------------------------------------------------------------------
    # 0. Basic checks & DW config
    # ---------------------------------------------------------------------
    local_vars = locals()

    if "trip_features" not in locals():
        raise RuntimeError(
            "[C11] trip_features not found inside run_pipeline(). "
            "This usually means F3/F4 did not run successfully."
        )

    if "df_clean" not in locals():
        raise RuntimeError(
            "[C11] df_clean not found inside run_pipeline(). "
            "This usually means C8 did not run successfully."
        )

    if "booking_id" not in trip_features.columns:
        raise ValueError(
            "[C11] ERROR: 'booking_id' column is missing from trip_features. "
            "Cannot populate Dim_Sensor without the natural key."
        )

    # Data Warehouse (Dim_Sensor) connection (warehouse)
    DW_USER = os.getenv("DW_USER", db_user)
    DW_PASS = os.getenv("DW_PASS", db_pass)
    DW_HOST = os.getenv("DW_HOST", db_host)
    DW_PORT = int(os.getenv("DW_PORT", db_port))
    DW_DB = os.getenv("DW_DB", "warehouse")
    DW_TABLE = os.getenv("DW_TABLE", "Dim_Sensor")  # must match your DW DDL
    quoted_dw_table = quote_qualified_identifier(DW_DB, DW_TABLE)

    print(f"[C11] DW target: {DW_HOST}:{DW_PORT}/{DW_DB}.{DW_TABLE}")

    # ---------------------------------------------------------------------
    # 1. Aggregate RAW sensor values per booking_id (from df_clean)
    # ---------------------------------------------------------------------
    df_source = df_clean  # <- use fully cleaned data, not df_typed

    raw_cols_required = [
        "booking_id",
        "accuracy",
        "bearing",
        "acceleration_x",
        "acceleration_y",
        "acceleration_z",
        "gyro_x",
        "gyro_y",
        "gyro_z",
        "second",
        "speed",
    ]

    missing_raw = [c for c in raw_cols_required if c not in df_source.columns]
    if missing_raw:
        raise ValueError(
            "[C11] df_clean is missing required raw columns for Dim_Sensor aggregation: "
            f"{missing_raw}"
        )

    raw_base = (
        df_source.select(raw_cols_required)
        .withColumn("booking_id", F.col("booking_id").cast("bigint"))
        .withColumn("accuracy", F.col("accuracy").cast("double"))
        .withColumn("bearing", F.col("bearing").cast("double"))
        .withColumn("acceleration_x", F.col("acceleration_x").cast("double"))
        .withColumn("acceleration_y", F.col("acceleration_y").cast("double"))
        .withColumn("acceleration_z", F.col("acceleration_z").cast("double"))
        .withColumn("gyro_x", F.col("gyro_x").cast("double"))
        .withColumn("gyro_y", F.col("gyro_y").cast("double"))
        .withColumn("gyro_z", F.col("gyro_z").cast("double"))
        .withColumn("second", F.col("second").cast("int"))
        .withColumn("speed", F.col("speed").cast("double"))
    )

    raw_agg = raw_base.groupBy("booking_id").agg(
        F.avg("accuracy").alias("accuracy"),
        F.avg("bearing").alias("bearing"),
        F.avg("acceleration_x").alias("acceleration_x"),
        F.avg("acceleration_y").alias("acceleration_y"),
        F.avg("acceleration_z").alias("acceleration_z"),
        F.avg("gyro_x").alias("gyro_x"),
        F.avg("gyro_y").alias("gyro_y"),
        F.avg("gyro_z").alias("gyro_z"),
        F.max("second").cast("int").alias("second"),  # last second in trip
        F.avg("speed").alias("speed"),  # average speed over trip
    )

    print("[C11] raw_agg columns:", raw_agg.columns)

    # ---------------------------------------------------------------------
    # 2. Join FE_* trip_features with raw_agg on booking_id
    # ---------------------------------------------------------------------
    trip_base = trip_features.withColumn(
        "booking_id", F.col("booking_id").cast("bigint")
    )

    trip_export = trip_base.alias("fe").join(
        raw_agg.alias("raw"), on="booking_id", how="left"
    )

    print("[C11] trip_export columns after join:", trip_export.columns)

    # ---------------------------------------------------------------------
    # 3. Cast numerics & add ETL timestamp
    # ---------------------------------------------------------------------
    numeric_cols = [
        "accuracy",
        "bearing",
        "acceleration_x",
        "acceleration_y",
        "acceleration_z",
        "gyro_x",
        "gyro_y",
        "gyro_z",
        "second",
        "speed",
        "FE_n_samples",
        "FE_trip_duration_s",
        "FE_sampling_rate_hz",
        "FE_speed_mean",
        "FE_speed_std",
        "FE_speed_max",
        "FE_speed_min",
        "FE_speed_p90",
        "FE_speed_change_rate_med",
        "FE_acc_mag_mean",
        "FE_acc_mag_std",
        "FE_acc_mag_max",
        "FE_acc_mag_p95",
        "FE_acc_x_mean",
        "FE_acc_y_mean",
        "FE_acc_z_mean",
        "FE_acc_x_std",
        "FE_acc_y_std",
        "FE_acc_z_std",
        "FE_jerk_med",
        "FE_jerk_p95",
        "FE_gyro_mag_mean",
        "FE_gyro_mag_std",
        "FE_gyro_mag_p95",
        "FE_turn_delta_std_deg",
        "FE_turn_rate_med_deg_s",
        "FE_gps_acc_mean",
        "FE_gps_acc_std",
        "FE_start_speed",
        "FE_end_speed",
    ]

    for col_name in numeric_cols:
        if col_name in trip_export.columns:
            if col_name in ["second", "FE_n_samples", "FE_trip_duration_s"]:
                trip_export = trip_export.withColumn(
                    col_name, F.col(col_name).cast("int")
                )
            else:
                trip_export = trip_export.withColumn(
                    col_name, F.col(col_name).cast("double")
                )

    trip_export = trip_export.withColumn("date_cleaned", F.current_timestamp())

    # ---------------------------------------------------------------------
    # 4. Align columns to Dim_Sensor schema (original + FE_* + date_cleaned)
    # ---------------------------------------------------------------------
    dim_sensor_cols = [
        "booking_id",
        "accuracy",
        "bearing",
        "acceleration_x",
        "acceleration_y",
        "acceleration_z",
        "gyro_x",
        "gyro_y",
        "gyro_z",
        "second",
        "speed",
        "FE_n_samples",
        "FE_trip_duration_s",
        "FE_sampling_rate_hz",
        "FE_speed_mean",
        "FE_speed_std",
        "FE_speed_max",
        "FE_speed_min",
        "FE_speed_p90",
        "FE_speed_change_rate_med",
        "FE_acc_mag_mean",
        "FE_acc_mag_std",
        "FE_acc_mag_max",
        "FE_acc_mag_p95",
        "FE_acc_x_mean",
        "FE_acc_y_mean",
        "FE_acc_z_mean",
        "FE_acc_x_std",
        "FE_acc_y_std",
        "FE_acc_z_std",
        "FE_jerk_med",
        "FE_jerk_p95",
        "FE_gyro_mag_mean",
        "FE_gyro_mag_std",
        "FE_gyro_mag_p95",
        "FE_turn_delta_std_deg",
        "FE_turn_rate_med_deg_s",
        "FE_gps_acc_mean",
        "FE_gps_acc_std",
        "FE_start_speed",
        "FE_end_speed",
        "date_cleaned",
    ]

    # Ensure all expected columns exist (add NULLs if missing, just in case)
    for col_name in dim_sensor_cols:
        if col_name not in trip_export.columns:
            if col_name in [
                "booking_id",
                "FE_n_samples",
                "FE_trip_duration_s",
                "second",
            ]:
                trip_export = trip_export.withColumn(col_name, F.lit(None).cast("int"))
            elif col_name == "date_cleaned":
                trip_export = trip_export.withColumn(col_name, F.current_timestamp())
            else:
                trip_export = trip_export.withColumn(
                    col_name, F.lit(None).cast("double")
                )

    trip_export = trip_export.select(dim_sensor_cols)
    dw_cols = dim_sensor_cols
    print("[C11] DW column order:", dw_cols)

    # ---------------------------------------------------------------------
    # 5. Build INSERT statement
    # ---------------------------------------------------------------------
    placeholders = ", ".join(["%s"] * len(dw_cols))
    col_list = ", ".join(f"`{c}`" for c in dw_cols)

    insert_sql = sql_statement(
        "INSERT INTO",
        quoted_dw_table,
        f"({col_list})",
        "VALUES",
        f"({placeholders})",
    )

    # ---------------------------------------------------------------------
    # 6. Connect to DW and stream-insert in batches
    # ---------------------------------------------------------------------
    dw_conn = mysql.connector.connect(
        host=DW_HOST,
        port=DW_PORT,
        user=DW_USER,
        password=DW_PASS,
        database=DW_DB,
    )
    dw_cur = dw_conn.cursor()

    BATCH_SIZE = 1000
    batch = []
    total_inserted = 0
    booking_ids_set = set()

    print("[C11] Starting streaming insert from Spark → DW.Dim_Sensor ...")

    for row in trip_export.toLocalIterator():
        row_tuple = tuple(row[c] for c in dw_cols)
        batch.append(row_tuple)
        booking_ids_set.add(row["booking_id"])

        if len(batch) >= BATCH_SIZE:
            dw_cur.executemany(insert_sql, batch)
            dw_conn.commit()
            total_inserted += len(batch)
            print(f"[C11] Inserted batch, cumulative rows: {total_inserted}")
            batch = []

    # Flush remaining rows
    if batch:
        dw_cur.executemany(insert_sql, batch)
        dw_conn.commit()
        total_inserted += len(batch)
        print(
            f"[C11] Final batch inserted. Total rows inserted/updated: {total_inserted}"
        )

    dw_cur.close()
    dw_conn.close()
    print("[C11] DW Dim_Sensor load complete.")

    # ---------------------------------------------------------------------
    # 7. Mark staging.sensor_data_source rows as cleaned
    # ---------------------------------------------------------------------
    booking_ids = [bid for bid in booking_ids_set if bid is not None]
    print(f"[C11] Unique booking_ids for staging update: {len(booking_ids)}")

    if booking_ids:
        staging_conn = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_pass,
            database=db_name,  # e.g. "staging"
        )
        staging_cur = staging_conn.cursor()

        chunk_size = 1000
        total_updated = 0

        for i in range(0, len(booking_ids), chunk_size):
            chunk = booking_ids[i : i + chunk_size]
            placeholders = ", ".join(["%s"] * len(chunk))

            sql = sql_statement(
                "UPDATE",
                quoted_db_table,
                "SET cleaned_date = NOW()",
                "WHERE booking_id IN",
                f"({placeholders})",
            )

            staging_cur.execute(sql, chunk)
            staging_conn.commit()
            total_updated += staging_cur.rowcount
            print(
                f"[C11] Staging UPDATE chunk done, cumulative updated: {total_updated}"
            )

        staging_cur.close()
        staging_conn.close()
        print(
            "[C11] Staging update complete — cleaned_date set for approximately",
            total_updated,
            "rows in",
            repr(db_table),
        )
    else:
        print("[C11] No booking_ids collected — staging.cleaned_date not updated.")


def main():
    """CLI entry point."""
    logger.info("Starting PAI Sensor ETL Pipeline...")
    try:
        run_pipeline()
        logger.info("Pipeline completed successfully.")
    except Exception as exc:
        logger.exception("Pipeline failed with an exception: %s", exc)
        raise


if __name__ == "__main__":
    main()
