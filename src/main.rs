use rand::Rng;
use uuid::Uuid;

use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;
use arrow::array::{ArrayRef, Int64Array, Int32Array, StringArray, DictionaryArray};

use parquet::arrow::*;
use parquet::file::properties::WriterProperties;
use parquet::basic::Compression;

use std::{env, fs};
use std::fs::File;
use std::sync::Arc;
use std::path::Path;


fn create_random_batch(size: i32) -> RecordBatch {
    // create column `a`
    let mut values_vector = Vec::new();
    for _ in 1..=size {
        values_vector.push(String::from(Uuid::new_v4().to_string()));
    }
    let values = StringArray::from(values_vector);
    let mut keys_vector: Vec<i32> = Vec::new();
    for _ in 1..=size {
        keys_vector.push(rand::thread_rng().gen_range(0..size));
    }
    let keys = Int32Array::from(keys_vector);
    let a: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap());

    // create column `b`
    let mut values: Vec<i64> = Vec::new();
    for _ in 1..=size {
        values.push(rand::thread_rng().gen_range(0..size) as i64);
    }
    let b: ArrayRef = Arc::new(Int64Array::from(values));
    return RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("Usage: {} <num_files> <destination_dir>", args[0]);
    }

    let num_files = args[1].parse::<i32>().unwrap();
    let destination_dir = &args[2];

    fs::create_dir_all(destination_dir).expect("Creating destination directory");

    let sample_batch = create_random_batch(5);

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    for i in 1..=num_files {
        let path = Path::new(destination_dir).join(format!("{}.parquet", i));
        let file = File::create(path).expect("Creating file");
        let mut writer = ArrowWriter::try_new(file, sample_batch.schema(), Some(props.clone())).unwrap();

        for _ in 1..=500 {
            let batch = create_random_batch(8192);
            writer.write(&batch).expect("Writing batch");
        }

        writer.close().unwrap();
    }

}

