"""
ETL Pipeline for Sunglass Store Data
Extracts data from S3, loads into PostgreSQL, and runs dbt transformations.
"""
import os
import sys
import dlt
from dlt.sources.filesystem import filesystem, read_parquet
from typing import Optional
from models import Users, Products, Orders, Interactions, InteractionTypes
from dotenv import load_dotenv

load_dotenv(dotenv_path='../.env')

# Configuration constants
try:
    FILE_GLOB = os.environ["FILE_GLOB"]
    PIPELINE_NAME = os.environ["PIPELINE_NAME"]
    RAW_DATASET = os.environ["RAW_DATASET"]
    DLT_DATASET = os.environ["DLT_DATASET"]
    DEV_DBT_DATASET = os.environ["DEV_DBT_DATASET"]
    DESTINATION = os.environ["DESTINATION"]
except KeyError as e:
    print(f"Error: Missing required environment variable {e}")
    sys.exit(1)
    
    
def get_pipeline(pipeline_name=PIPELINE_NAME, destination=DESTINATION, dataset_name=RAW_DATASET):
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
    )

def create_source(
    path: str, 
    table_name: str, 
    write_disposition: dict | str, 
    merge_key: Optional[str] = None, 
    primary_key: Optional[str] = None,
    incremental_column: Optional[str] = None,
):
    """
    Create a dlt source with specified configuration.
    
    Args:
        path: S3 path to the data
        table_name: Target table name
        write_disposition: Write strategy (merge, append, etc.)
        merge_key: Key for merge operations
        primary_key: Key for merge operations
        incremental_column: Column for incremental loads
        columns: Column definitions with data types for type hints
    
    Raises:
        KeyError: If S3_BASE_URL environment variable is not set
        Exception: If source creation fails
    """
    try:
        base_url = os.environ["S3_BASE_URL"]
        source = (
            filesystem(bucket_url=base_url + path, file_glob=FILE_GLOB) 
            | read_parquet()
        )
        
        hints = {"table_name": table_name, "write_disposition": write_disposition}
        
        if merge_key:
            hints["merge_key"] = merge_key
        
        if primary_key:
            hints["primary_key"] = primary_key
        
        if incremental_column:
            hints["incremental"] = dlt.sources.incremental(incremental_column)
        
        return source.apply_hints(**hints)
    
    except KeyError as e:
        print(f"Error: Missing environment variable {e}")
        raise
    except Exception as e:
        print(f"Error creating source for {table_name}: {str(e)}")
        raise

@dlt.resource(columns=Users, name="users")
def get_users():
    """Create users dimension source with SCD Type 2 strategy."""
    yield from create_source(
        path=os.environ["USERS_PATH"],
        table_name="users",
        # write_disposition={
        #     "disposition": "merge",
        #     "strategy": "scd2",
        #     "validity_column_names": ["valid_from", "valid_to"]
        # },
        write_disposition="replace",
        # merge_key="user_id"
    )


@dlt.resource(columns=Products, name="products")
def get_products():
    """Create products dimension source with SCD Type 2 strategy."""
    yield from create_source(
        path=os.environ["PRODUCTS_PATH"],
        table_name="products",
        # write_disposition={
        #     "disposition": "merge",
        #     "strategy": "scd2",
        #     "validity_column_names": ["valid_from", "valid_to"]
        # },
        # merge_key="item_id",
        write_disposition="replace",
    )


@dlt.resource(columns=Orders, name="orders")
def get_orders():
    """Create orders fact source with incremental append strategy."""
    yield from create_source(
        path=os.environ["ORDERS_PATH"],
        table_name="orders",
        write_disposition="replace",
        incremental_column="purchase_date"
    )


@dlt.resource(columns=Interactions, name="interactions")
def get_interactions():
    """Create interactions fact source with incremental append strategy."""
    yield from create_source(
        path=os.environ["INTERACTIONS_PATH"],
        table_name="interaction",
        write_disposition="replace",
        incremental_column="interaction_date"
    )


@dlt.resource(columns=InteractionTypes, name="interaction_types")
def get_interaction_types():
    """Create interaction types reference source with upsert strategy."""
    yield from create_source(
        path=os.environ["INTERACTION_TYPES_PATH"],
        table_name="interaction_types",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        primary_key="id"
    )


def extract_and_load():
    """
    Extract data from S3 and load into PostgreSQL.
    
    Returns:
        dlt.Pipeline: The pipeline object after successful load
    
    Raises:
        KeyError: If required environment variables are missing
        Exception: If pipeline execution fails
    """
    try:
        print("Creating pipeline and loading data...")
        
        # Create pipeline and load data
        pipeline = get_pipeline()
        
        log = pipeline.run([get_users(), get_products(), get_orders(), get_interactions(), get_interaction_types()])
        print(f"ETL Load completed:\n{log}")
        
        return pipeline
    
    except KeyError as e:
        print(f"Error: Missing environment variable {e}")
        print("Required variables: USERS_PATH, PRODUCTS_PATH, ORDERS_PATH, INTERACTIONS_PATH, INTERACTION_TYPES_PATH")
        raise
    except Exception as e:
        print(f"Error during extract and load: {str(e)}")
        raise


def run_dbt_transformations():
    """
    Run dbt transformations on loaded data.
    
    Raises:
        Exception: If dbt transformation fails
    """
    try:
        print("🔧 Setting up dbt environment...")
        
        # Create separate pipeline for dbt with DBT_DATASET as target schema
        dbt_pipeline = get_pipeline(
            pipeline_name=f"{PIPELINE_NAME}_dbt",
            destination=DESTINATION,
            dataset_name=DEV_DBT_DATASET
        )
        
        dbt = dlt.dbt.package(
            pipeline=dbt_pipeline,
            package_location="../dbt_sunglass_store"
        )
        
        print("🔄 Running dbt models...")
        
        # Run all dbt models
        models = dbt.run_all()
        
        print("\n=== dbt Transformation Results ===")
        failed_models = []
        
        for model in models:
            status_icon = "✔" if model.status == "success" else "✘"
            print(
                f"{status_icon} Model: {model.model_name}\n"
                f"  Time: {model.time}\n"
                f"  Status: {model.status}\n"
                f"  Message: {model.message}\n"
            )
            
            if model.status != "success":
                failed_models.append(model.model_name)
        
        if failed_models:
            raise Exception(f"The following dbt models failed: {', '.join(failed_models)}")
        
        print("All dbt transformations completed successfully!")
    
    except FileNotFoundError as e:
        print(f"Error: dbt project not found at '../dbt_sunglass_store'")
        print(f"Details: {str(e)}")
        raise
    except Exception as e:
        print(f"Error during dbt transformations: {str(e)}")
        raise


def main():
    """
    Main pipeline execution with error handling.
    
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    try:
        print("Starting Sunglass Store ETL Pipeline")
        
        # Extract and Load
        print("Step 1: Extracting and loading data...")
        extract_and_load()
        
        # Transform with dbt
        print("\nStep 2: Running dbt transformations...")
        run_dbt_transformations()
        
        print("Pipeline completed successfully!")
        
        return 0
    
    except Exception as e:
        print(f"\n Pipeline failed with error: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)