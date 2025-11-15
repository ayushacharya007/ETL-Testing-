"""
ETL Pipeline for Sunglass Store Data
Extracts data from S3, loads into PostgreSQL, and runs dbt transformations.
"""
import dlt
from dlt.sources.filesystem import filesystem, read_parquet
from typing import Optional
import os
import sys
from dotenv import load_dotenv

load_dotenv(dotenv_path='../.env')

# Configuration constants
try:
    FILE_GLOB = os.environ["FILE_GLOB"]
    PIPELINE_NAME = os.environ["PIPELINE_NAME"]
    RAW_DATASET = os.environ["RAW_DATASET"]
    DBT_DATASET = os.environ["DBT_DATASET"]
except KeyError as e:
    print(f"❌ Error: Missing required environment variable {e}")
    print("Please ensure all required variables are set in .env file")
    sys.exit(1)

def create_source(
    path: str, 
    table_name: str, 
    write_disposition: dict | str, 
    primary_key: Optional[str] = None, 
    incremental_column: Optional[str] = None,
    columns: Optional[dict] = None
):
    """
    Create a dlt source with specified configuration.
    
    Args:
        path: S3 path to the data
        table_name: Target table name
        write_disposition: Write strategy (merge, append, etc.)
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
        
        if primary_key:
            hints["primary_key"] = primary_key
        
        if incremental_column:
            hints["incremental"] = dlt.sources.incremental(incremental_column)
        
        if columns:
            hints["columns"] = columns
        
        return source.apply_hints(**hints)
    
    except KeyError as e:
        print(f"❌ Error: Missing environment variable {e}")
        raise
    except Exception as e:
        print(f"❌ Error creating source for {table_name}: {str(e)}")
        raise


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
        print("📦 Creating data sources...")
        
        # Define sources with SCD2 strategy for dimension tables
        user = create_source(
            path=os.environ["USERS_PATH"],
            table_name="users",
            write_disposition={
                "disposition": "merge",
                "strategy": "scd2",
                "validity_column_names": ["valid_from", "valid_to"]
            },
            primary_key="user_id"
        )
        
        product = create_source(
            path=os.environ["PRODUCTS_PATH"],
            table_name="products",
            write_disposition={
                "disposition": "merge",
                "strategy": "scd2",
                "validity_column_names": ["valid_from", "valid_to"]
            },
            primary_key="item_id"
        )
        
        # Append-only for fact tables with incremental loads
        order = create_source(
            path=os.environ["ORDERS_PATH"],
            table_name="orders",
            write_disposition="append",
            incremental_column="purchase_date"
        )
        
        # Interaction table with explicit column type hints
        interaction = create_source(
            path=os.environ["INTERACTIONS_PATH"],
            table_name="interaction",
            write_disposition="append",
            incremental_column="interaction_date",
            columns={
                "interaction_type": {"data_type": "text", "nullable": True}
            }
        )
        
        # Upsert for reference tables
        interaction_type = create_source(
            path=os.environ["INTERACTION_TYPES_PATH"],
            table_name="interactionTypes",
            write_disposition={"disposition": "merge", "strategy": "upsert"},
            primary_key="id"
        )
        
        print("🚀 Creating pipeline and loading data...")
        
        # Create pipeline and load data
        pipeline = dlt.pipeline(
            pipeline_name=PIPELINE_NAME,
            destination="postgres",
            dataset_name=RAW_DATASET,
            dev_mode=True
        )
        
        log = pipeline.run([user, product, order, interaction, interaction_type])
        print(f"✅ ETL Load completed:\n{log}")
        
        return pipeline
    
    except KeyError as e:
        print(f"❌ Error: Missing environment variable {e}")
        print("Required variables: USERS_PATH, PRODUCTS_PATH, ORDERS_PATH, INTERACTIONS_PATH, INTERACTION_TYPES_PATH")
        raise
    except Exception as e:
        print(f"❌ Error during extract and load: {str(e)}")
        raise


def run_dbt_transformations():
    """
    Run dbt transformations on loaded data.
    
    Raises:
        Exception: If dbt transformation fails
    """
    try:
        print("🔧 Setting up dbt environment...")
        
        pipeline = dlt.pipeline(
            pipeline_name=PIPELINE_NAME,
            destination="postgres",
            dataset_name=DBT_DATASET
        )
        
        # Set up dbt environment
        venv = dlt.dbt.get_venv(pipeline=pipeline)
        
        dbt = dlt.dbt.package(
            pipeline=pipeline,
            package_location="../dbt_sunglass_store"
        )
        
        print("🔄 Running dbt models...")
        
        # Run all dbt models
        models = dbt.run_all()
        
        print("\n=== dbt Transformation Results ===")
        failed_models = []
        
        for model in models:
            status_icon = "✅" if model.status == "success" else "❌"
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
        
        print("✅ All dbt transformations completed successfully!")
    
    except FileNotFoundError as e:
        print(f"❌ Error: dbt project not found at '../dbt_sunglass_store'")
        print(f"Details: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ Error during dbt transformations: {str(e)}")
        raise


def main():
    """
    Main pipeline execution with error handling.
    
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    try:
        print("=" * 60)
        print("🎯 Starting Sunglass Store ETL Pipeline")
        print("=" * 60 + "\n")
        
        # Extract and Load
        print("Step 1: Extracting and loading data...")
        extract_and_load()
        
        # Transform with dbt
        print("\nStep 2: Running dbt transformations...")
        run_dbt_transformations()
        
        print("\n" + "=" * 60)
        print("🎉 Pipeline completed successfully!")
        print("=" * 60)
        
        return 0
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user")
        return 1
    
    except Exception as e:
        print("\n" + "=" * 60)
        print("❌ Pipeline failed!")
        print("=" * 60)
        print(f"Error: {str(e)}")
        print("\nPlease check:")
        print("  1. All environment variables are set in .env file")
        print("  2. Database credentials are correct in .dlt/secrets.toml")
        print("  3. S3 bucket access is properly configured")
        print("  4. dbt project exists at ../dbt_sunglass_store")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)