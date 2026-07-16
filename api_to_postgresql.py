from getpass import getpass
import logging

import pandas as pd
import requests
from sqlalchemy import (
    BigInteger,
    String,
    URL,
    create_engine,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


API_URL = "https://jsonplaceholder.typicode.com/users"

DB_USER = "postgres"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "Olist_db"

TARGET_SCHEMA = "public"
TARGET_TABLE = "api_users"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("api_to_postgresql.log"),
        logging.StreamHandler(),
    ],
)


def create_database_engine(password: str) -> Engine:
    """Create and return a SQLAlchemy engine for PostgreSQL."""

    logging.info("Creating PostgreSQL database engine.")

    database_url = URL.create(
        drivername="postgresql+psycopg",
        username=DB_USER,
        password=password,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
    )

    engine = create_engine(
        database_url,
        pool_pre_ping=True,
    )

    return engine


def test_database_connection(engine: Engine) -> None:
    """Test the PostgreSQL connection before starting the ETL process."""

    logging.info("Testing PostgreSQL connection.")

    with engine.connect() as connection:
        result = connection.execute(
            text(
                """
                SELECT
                    current_database(),
                    current_user;
                """
            )
        ).one()

    database_name = result[0]
    database_user = result[1]

    logging.info(
        "Connected successfully to database '%s' as user '%s'.",
        database_name,
        database_user,
    )


def extract() -> pd.DataFrame:
    """Extract user data from the API and return a raw DataFrame."""

    logging.info("Starting data extraction from API.")

    try:
        response = requests.get(
            API_URL,
            timeout=30,
        )

        response.raise_for_status()

        data = response.json()

        raw_df = pd.DataFrame(data)

        logging.info(
            "Extraction completed. Records extracted: %s",
            len(raw_df),
        )

        return raw_df

    except requests.Timeout as error:
        logging.error(
            "The API request timed out: %s",
            error,
        )
        raise

    except requests.HTTPError as error:
        logging.error(
            "The API returned an HTTP error: %s",
            error,
        )
        raise

    except requests.RequestException as error:
        logging.error(
            "The API request failed: %s",
            error,
        )
        raise

    except ValueError as error:
        logging.error(
            "The API response could not be converted from JSON: %s",
            error,
        )
        raise


def transform(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Select, clean, and standardize the required user data."""

    logging.info("Starting data transformation.")

    required_columns = [
        "id",
        "name",
        "username",
        "email",
        "phone",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in raw_df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Required columns are missing: {missing_columns}"
        )

    clean_df = raw_df[required_columns].copy()

    clean_df = clean_df.drop_duplicates()

    clean_df = clean_df.dropna(
        subset=["id", "name", "email"]
    )

    clean_df["id"] = pd.to_numeric(
        clean_df["id"],
        errors="raise",
    ).astype("int64")

    clean_df["name"] = (
        clean_df["name"]
        .astype("string")
        .str.strip()
    )

    clean_df["username"] = (
        clean_df["username"]
        .astype("string")
        .str.strip()
    )

    clean_df["email"] = (
        clean_df["email"]
        .astype("string")
        .str.strip()
        .str.lower()
    )

    clean_df["phone"] = (
        clean_df["phone"]
        .astype("string")
        .str.strip()
    )

    logging.info(
        "Transformation completed. Records remaining: %s",
        len(clean_df),
    )

    return clean_df


def validate(clean_df: pd.DataFrame) -> None:
    """Validate the cleaned DataFrame before loading it."""

    logging.info("Starting data validation.")

    if clean_df.empty:
        raise ValueError(
            "Validation failed: the DataFrame is empty."
        )

    if clean_df["id"].isnull().any():
        raise ValueError(
            "Validation failed: missing user IDs found."
        )

    if clean_df["id"].duplicated().any():
        duplicate_ids = (
            clean_df.loc[
                clean_df["id"].duplicated(keep=False),
                "id",
            ]
            .tolist()
        )

        raise ValueError(
            f"Validation failed: duplicate IDs found: "
            f"{duplicate_ids}"
        )

    if clean_df["name"].str.len().eq(0).any():
        raise ValueError(
            "Validation failed: blank names found."
        )

    if clean_df["email"].str.len().eq(0).any():
        raise ValueError(
            "Validation failed: blank emails found."
        )

    logging.info(
        "Data validation completed successfully."
    )


def load_to_postgresql(
    clean_df: pd.DataFrame,
    engine: Engine,
) -> None:
    """Load the cleaned DataFrame into PostgreSQL."""

    logging.info(
        "Starting PostgreSQL load into %s.%s.",
        TARGET_SCHEMA,
        TARGET_TABLE,
    )

    try:
        with engine.begin() as connection:
            clean_df.to_sql(
                name=TARGET_TABLE,
                con=connection,
                schema=TARGET_SCHEMA,
                if_exists="replace",
                index=False,
                dtype={
                    "id": BigInteger(),
                    "name": String(length=150),
                    "username": String(length=100),
                    "email": String(length=255),
                    "phone": String(length=100),
                },
                chunksize=1000,
                method="multi",
            )

        logging.info(
            "PostgreSQL load completed. Rows loaded: %s",
            len(clean_df),
        )

    except SQLAlchemyError as error:
        logging.error(
            "PostgreSQL load failed: %s",
            error,
        )
        raise


def verify_load(engine: Engine) -> None:
    """Verify that the expected data was loaded into PostgreSQL."""

    logging.info("Verifying PostgreSQL load.")

    query = text(
        f"""
        SELECT COUNT(*)
        FROM {TARGET_SCHEMA}.{TARGET_TABLE};
        """
    )

    with engine.connect() as connection:
        row_count = connection.execute(
            query
        ).scalar_one()

    logging.info(
        "Verification completed. PostgreSQL row count: %s",
        row_count,
    )


def main() -> None:
    """Run the complete API-to-PostgreSQL ETL pipeline."""

    logging.info(
        "API-to-PostgreSQL ETL pipeline started."
    )

    engine = None

    try:
        password = getpass(
            "Enter PostgreSQL password: "
        )

        engine = create_database_engine(password)

        test_database_connection(engine)

        raw_df = extract()

        clean_df = transform(raw_df)

        validate(clean_df)

        load_to_postgresql(
            clean_df,
            engine,
        )

        verify_load(engine)

    except Exception:
        logging.exception(
            "API-to-PostgreSQL ETL pipeline failed."
        )
        raise

    else:
        logging.info(
            "API-to-PostgreSQL ETL pipeline "
            "completed successfully."
        )

    finally:
        if engine is not None:
            engine.dispose()

            logging.info(
                "Database engine disposed."
            )


if __name__ == "__main__":
    main()