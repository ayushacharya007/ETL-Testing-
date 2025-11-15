# flake8: noqa
import dlt
from dlt.sources.filesystem import filesystem, read_parquet


base_url = "s3://demo-sunglass-store-180294223557/sunglass_store/"
file_glob= "**/*.parquet"

# Extract data from filesystem source and apply hints for Postgres destination
user = (filesystem(bucket_url=base_url + "users73de7f11c366/data/Fq_pVQ/", file_glob=file_glob) | read_parquet()).apply_hints(table_name="users", write_disposition={"disposition":"merge", "strategy":"scd2", "validity_column_names": ["valid_from", "valid_to"]}, merge_key="user_id")

product = (filesystem(bucket_url=base_url + "products4f4874b1cda7/data/CyMCPw/", file_glob=file_glob) | read_parquet()).apply_hints(table_name="products", write_disposition={"disposition":"merge", "strategy":"scd2", "validity_column_names": ["valid_from", "valid_to"]}, merge_key="item_id")

order = (filesystem(bucket_url=base_url + "orderse4533be1acb8/data/JVM1IQ/", file_glob=file_glob) | read_parquet()).apply_hints(table_name="orders", write_disposition="append", incremental=dlt.sources.incremental("purchase_date"))

interaction = (filesystem(bucket_url=base_url + "interactionsc81c93d1c118/data/tRbgVQ/", file_glob=file_glob) | read_parquet()).apply_hints(table_name="interaction", write_disposition="append", incremental=dlt.sources.incremental("interaction_date"))

interaction_type = (filesystem(bucket_url=base_url + "interaction_typesfbd7a895a998/data/m72zYQ/", file_glob=file_glob) | read_parquet()).apply_hints(table_name="interactionTypes", write_disposition={"disposition":"merge", "strategy":"upsert"}, primary_key="id")


pipeline = dlt.pipeline(
    pipeline_name="sunglass_store", 
    destination="postgres",
    dataset_name="sunglass_store_raw"
)

log = pipeline.run(
    [user, product, order, interaction, interaction_type]
)

print(log)


# transform using dbt
pipeline = dlt.pipeline(
    pipeline_name="sunglass_store",
    destination="postgres",
    dataset_name="sunglass_store_dbt"
)

venv = dlt.dbt.get_venv(pipeline=pipeline)

dbt = dlt.dbt.package(
    pipeline=pipeline,
    package_location="../dbt_sunglass_store"
)

models = dbt.run_all()

for m in models:
    print(
        f"Model {m.model_name} materialized" +
        f" in {m.time}" +
        f" with status {m.status}" +
        f" and message {m.message}"
    )
    print('\n')

