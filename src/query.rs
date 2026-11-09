//! DataFusion over the archive: the archive's object store registered with the runtime and
//! `turns` as a vortex-datafusion ListingTable partitioned by `dt`.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use vortex::session::VortexSession;
use vortex_datafusion::VortexFormat;

use crate::store::{Archive, TURNS_TABLE};

/// A session with `turns` registered over `archive`.
pub async fn session_context(archive: &Archive, vortex: VortexSession) -> Result<SessionContext> {
    let ctx = SessionContext::new();
    ctx.register_object_store(&archive.url, archive.store.clone());

    let table_url = archive.table_url(TURNS_TABLE)?;
    let options = ListingOptions::new(Arc::new(VortexFormat::new(vortex)))
        .with_file_extension(".vortex")
        .with_table_partition_cols(vec![("dt".to_owned(), DataType::Utf8)]);
    let config = ListingTableConfig::new(ListingTableUrl::parse(table_url.as_str())?)
        .with_listing_options(options)
        .infer_schema(&ctx.state())
        .await
        .with_context(|| format!("inferring the turns schema from {table_url}"))?;
    let table = ListingTable::try_new(config)?;
    ctx.register_table(TURNS_TABLE, Arc::new(table))?;
    Ok(ctx)
}

/// Run one SQL statement and collect its result.
pub async fn run(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    let frame = ctx.sql(sql).await.context("planning the query")?;
    frame.collect().await.context("running the query")
}
