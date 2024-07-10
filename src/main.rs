use std::{fs::File, path::PathBuf, sync::Arc};

use clap::{Parser, Subcommand};
use datafusion::{
    arrow::{
        array::{RecordBatch, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
    },
    dataframe::DataFrameWriteOptions,
    execution::{
        config::SessionConfig,
        context::SessionContext,
        memory_pool::FairSpillPool,
        options::ParquetReadOptions,
        runtime_env::{RuntimeConfig, RuntimeEnv},
    },
    logical_expr::col,
    parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties},
};
use rand::Rng;

#[derive(Parser, Debug)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug, Clone)]
enum Command {
    Generate {
        output_file: PathBuf,
    },
    Deduplicate {
        source_file: PathBuf,
        output_file: PathBuf,
    },
}

fn schema() -> Arc<Schema> {
    return Arc::new(Schema::new(vec![
        Field::new("ts", DataType::UInt64, false),
        Field::new("message", DataType::Utf8, false),
    ]));
}

fn generate(output_file: PathBuf) {
    let schema = schema();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(
        File::create_new(output_file).expect("Opening output file failed"),
        schema.clone(),
        Some(props),
    )
    .unwrap();

    let batch_size = 1024;

    let message = "Fo bar soome string, not sure if this is really needed!";
    let message_col = Arc::new(StringArray::from_iter_values(
        (0..batch_size).map(|_| &message),
    ));

    let mut rng = rand::thread_rng();

    for _ in 0..100_000 {
        let ts_col = Arc::new(UInt64Array::from_iter_values(
            (0..batch_size).map(|_| rng.gen()),
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![ts_col, message_col.clone()])
            .expect("failed to create batch");

        writer.write(&batch).expect("Failed to write record");
    }
    writer.finish().expect("failed to finish");
}

async fn deduplicate(source: PathBuf, dest: PathBuf) {
    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::new(),
        Arc::new(
            RuntimeEnv::new(
                RuntimeConfig::new()
                    .with_temp_file_path(
                        "temp-dir", // ensure we do not write to /tmp
                    )
                    .with_memory_pool(Arc::new(FairSpillPool::new(1_000_000_000))), // 1 GB
            )
            .unwrap(),
        ),
    );
    let df = ctx
        .read_parquet(
            source.to_str().expect("Failed to convert path to &str"),
            ParquetReadOptions::default(),
        )
        .await
        .expect("Failed to read intermediate output for filterig")
        .distinct_on(
            vec![col("ts"), col("message")],
            vec![col("ts"), col("message")],
            None,
        )
        .expect("filtering duplicates failed");

    df.write_parquet(
        dest.to_str().expect("failed to convert output path"),
        DataFrameWriteOptions::new(),
        None,
    )
    .await
    .expect("Failure to write back final result");

    println!("done (not printed!)");
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    match args.command {
        Command::Generate { output_file } => generate(output_file),
        Command::Deduplicate {
            source_file,
            output_file,
        } => deduplicate(source_file, output_file).await,
    }
}
