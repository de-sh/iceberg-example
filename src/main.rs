use std::{collections::HashMap, env, sync::Arc};

use arrow::{
    array::{Float64Array, Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use iceberg::{
    Catalog, NamespaceIdent, TableCreation, TableIdent,
    arrow::arrow_schema_to_schema,
    io::{FileIO, FileIOBuilder},
    spec::DataFileFormat,
    table::Table,
    transaction::Transaction,
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
        },
    },
};
use iceberg_catalog_memory::MemoryCatalog;
use parquet::{arrow::PARQUET_FIELD_ID_META_KEY, file::properties::WriterProperties};

// 1. Setup function to initialize catalog and FileIO
async fn setup_catalog() -> iceberg::Result<(FileIO, impl Catalog)> {
    let file_io = FileIOBuilder::new_fs_io().build()?;
    let catalog = MemoryCatalog::new(file_io.clone(), None);
    Ok((file_io, catalog))
}

// 2. Function to create Iceberg table with specific schema
async fn create_iceberg_table(
    catalog: &impl Catalog,
    namespace_name: &str,
    table_name: &str,
    arrow_schema: &Schema,
    properties: HashMap<String, String>,
) -> iceberg::Result<Table> {
    let iceberg_schema = arrow_schema_to_schema(arrow_schema)?;
    // Create namespace if doesn't exist
    let ns_ident = NamespaceIdent::new(namespace_name.to_owned());
    if catalog.get_namespace(&ns_ident).await.is_err() {
        catalog
            .create_namespace(&ns_ident, Default::default())
            .await?;
    }

    // Create table ident
    let table_id = TableIdent::from_strs([namespace_name, table_name])?;

    // Build table creation parameters
    let table_creation = TableCreation::builder()
        .name(table_id.name)
        .schema(iceberg_schema)
        .location(format!(
            "{}/{table_name}",
            env::var("ICEBERG_ROOT").expect("Please set the env var ICEBERG_ROOT")
        ))
        .properties(properties)
        .build();

    // Create table
    let table = catalog.create_table(&ns_ident, table_creation).await?;
    Ok(table)
}

// 3. Generic function to write Arrow data to Iceberg table
async fn write_arrow_data(
    table: &Table,
    catalog: &impl Catalog,
    batch: RecordBatch,
    file_io: &FileIO,
) -> iceberg::Result<()> {
    // Create location generator from table metadata
    let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;

    // Create file writer components
    let file_name_gen =
        DefaultFileNameGenerator::new("<something>".into(), None, DataFileFormat::Parquet);
    let file_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        Arc::new(table.metadata().current_schema().as_ref().clone()),
        file_io.clone(),
        location_gen,
        file_name_gen,
    );

    // Create data writer and write batch
    let mut writer = DataFileWriterBuilder::new(file_writer_builder, None)
        .build()
        .await?;

    writer.write(batch).await?;
    let data_files = writer.close().await?;

    // Add the data files to the transaction
    let transaction = Transaction::new(table);
    let mut append = transaction.fast_append(None, vec![])?;
    append.add_data_files(data_files)?;
    let transaction = append.apply().await?;

    // Commit the transaction to create a new snapshot
    transaction.commit(catalog).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize catalog and FileIO
    let (file_io, catalog) = setup_catalog().await?;

    // Create first schema and table
    let schema1 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false).with_metadata(HashMap::from_iter([(
            PARQUET_FIELD_ID_META_KEY.to_owned(),
            "1".to_owned(),
        )])),
        Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from_iter([(
            PARQUET_FIELD_ID_META_KEY.to_owned(),
            "2".to_owned(),
        )])),
    ]));

    let table1 = create_iceberg_table(
        &catalog,
        "test",
        "users",
        schema1.as_ref(),
        HashMap::from([("owner".into(), "team_a".into())]),
    )
    .await?;

    let batch1 = RecordBatch::try_new(
        schema1,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )?;

    // Write first dataset
    write_arrow_data(&table1, &catalog, batch1, &file_io).await?;

    // Create second schema and table
    let schema2 = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )
        .with_metadata(HashMap::from_iter([(
            PARQUET_FIELD_ID_META_KEY.to_owned(),
            "0".to_owned(),
        )])),
        Field::new("value", DataType::Float64, true).with_metadata(HashMap::from_iter([(
            PARQUET_FIELD_ID_META_KEY.to_owned(),
            "1".to_owned(),
        )])),
    ]));

    let table2 = create_iceberg_table(
        &catalog,
        "test",
        "metrics",
        schema2.as_ref(),
        HashMap::from([("owner".into(), "team_b".into())]),
    )
    .await?;

    let batch2 = RecordBatch::try_new(
        schema2,
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![
                1713137000, 1713137001, 1713137002,
            ])),
            Arc::new(Float64Array::from(vec![12.3, 45.6, 78.9])),
        ],
    )?;

    // Write second dataset
    write_arrow_data(&table2, &catalog, batch2, &file_io).await?;

    Ok(())
}
