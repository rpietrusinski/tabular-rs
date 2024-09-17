use polars::prelude::*;

fn main() -> PolarsResult<()> {
    let mut lf = LazyCsvReader::new("data/netflix_titles.csv").finish()?;

    // 1. print schema
    println!("{:?}", lf.collect_schema()?);

    // 2. Raw DF
    let raw_df = lf.clone().with_streaming(true).collect()?;
    println!("Raw DF: {}", raw_df);

    // 3. Group by
    let produced_by_director = lf
        .clone()
        .drop_nulls(Some(vec![col("director")]))
        .group_by([col("director")])
        .agg([col("show_id").n_unique().alias("num_movies")])
        .sort(
            ["num_movies"],
            SortMultipleOptions::default().with_order_descending(true),
        )
        .with_streaming(true)
        .collect()?
        .head(Some(15));
    println!("Grouped: {}", produced_by_director);

    let release_year = lf
        .clone()
        .group_by([col("type")])
        .agg([
            col("release_year").min().alias("min_release_year"),
            col("release_year").max().alias("max_release_year"),
            col("release_year").mean().alias("mean_release_year"),
        ])
        .with_streaming(true)
        .collect()?;
    println!("Release year: {}", release_year);

    // 4. Oldest TV Shows
    let oldest_tv_show = lf
        .clone()
        .filter(col("type").eq(lit("TV Show")))
        .select([col("title"), col("release_year").cast(DataType::Float64)])
        .sort(["release_year"], Default::default())
        .collect()?
        .head(Some(5));
    println!("Oldest TV Shows: {}", oldest_tv_show);

    Ok(())
}
