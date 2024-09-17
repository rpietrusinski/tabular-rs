# Polars - Rust based DataFrame library - Walkthrough

This project covers basic functionality of `polars` library. It consists of 2 CSV datasets
(netflix movies & tv shows, wine catalogue). The pipelines introduce common data manipulation 
methods and serve for the purpose of learning writing such pipelines in Rust. 

## Examples
### 0. Read dataset
Data is scanned and LazyFrame object is created. Data is only loaded to memory once `collect` method is called.
A nice feature of Polars is streaming functionality. It enables to process data in batches, without 
reading it all to memory and enables to process datasets bigger than user's machine memory (see 
`with_streaming(true)` method below).
```rust
let mut lf = LazyCsvReader::new("data/wine.csv")
    .with_has_header(true)
    .finish()?;

let mut lf = LazyCsvReader::new("data/netflix_titles.csv").finish()?;
```

### 1. Netflix dataset
- Show top15 directors who filmed the most movied/shows
```rust
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
```

- Return summary stats for column `release_year`. Return min, max, avg release year in. Show results in 
separation for production type.
```rust
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
```

- Show 5 oldest TV Shows. Cast `release_year` to float type.
```rust
let oldest_tv_show = lf
    .clone()
    .filter(col("type").eq(lit("TV Show")))
    .select([col("title"), col("release_year").cast(DataType::Float64)])
    .sort(["release_year"], Default::default())
    .collect()?
    .head(Some(5));
println!("Oldest TV Shows: {}", oldest_tv_show);
```

### 2. Wine dataset
- Extract records with `alcohol > 14`. Return `alcohol` represented as `alcohol_level` column, relative ash,
total_phenols increased by 2 and total_phenols increased by 3 with UDF function.
```rust
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

fn add_3(x: Series) -> PolarsResult<Option<Series>> {
    match x.dtype() {
        DataType::Float64 => {
            let result = x.f64()?.apply_values(|y| y + 3.0).into_series();
            Ok(Some(result))
        }
        _ => panic!("Unexpected dtype"),
    }
}
```

- Show min & max values of 3 columns: `alcohol`, `total_phenols` & `color_intensity`.
```rust
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
```

