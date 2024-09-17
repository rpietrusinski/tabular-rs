use polars::prelude::*;

fn main() -> PolarsResult<()> {
    let mut lf = LazyCsvReader::new("data/wine.csv")
        .with_has_header(true)
        .finish()?;

    // 1. print schema
    println!("{:?}", lf.collect_schema()?);

    // 2. Raw DF
    let raw_df = lf.clone().with_streaming(true).collect()?;
    println!("Raw DF: {}", raw_df);

    // 3. Basic filtering and selection
    let subset = lf
        .clone()
        .filter(col("alcohol").gt(lit(14)))
        .select([
            col("alcohol").alias("alcohol_level"),
            (col("ash") / mean("ash")).alias("relative_ash"),
            (col("total_phenols") + lit(2)).alias("total_phenols+2"),
            col("total_phenols")
                .map(add_3, GetOutput::same_type())
                .alias("total_phenols_udf"),
        ])
        .with_streaming(true)
        .collect()?;
    println!("Basic filtering and selection: {}", subset);

    // 4. Basic aggregations
    let min_max_vals = lf
        .clone()
        .select([
            col("alcohol").min().alias("min_alcohol"),
            col("alcohol").max().alias("max_alcohol"),
            col("total_phenols").min().alias("min_total_phenols"),
            col("total_phenols").max().alias("max_total_phenols"),
            col("color_intensity").min().alias("min_color_intensity"),
            col("color_intensity").max().alias("max_color_intensity"),
        ])
        .with_streaming(true)
        .collect()?;
    println!("Basic aggregations: {}", min_max_vals);

    Ok(())
}

///Function applies some transformation to each element of a Series. In order to make it work
/// one needs to convert Series -> ChunkedArray, only after apply transformation and
/// return Result<Option<Series>> with the function.
fn add_3(x: Series) -> PolarsResult<Option<Series>> {
    match x.dtype() {
        DataType::Float64 => {
            let result = x.f64()?.apply_values(|y| y + 3.0).into_series();
            Ok(Some(result))
        }
        _ => panic!("Unexpected dtype"),
    }
}
