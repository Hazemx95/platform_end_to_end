# Data Platform

A local medallion-style data platform that ingests CSV extracts from business systems into layered storage, starting with the Bronze Layer.

## Language

### Sources

**Source System**:
A business system that produces extracts, currently `source_crm` and `source_erp`.
_Avoid_: source, system, origin

**Source Table**:
One logical dataset from a Source System, such as `cust_info` or `LOC_A101`, delivered as one or more Source Files over time.
_Avoid_: dataset, entity, feed

**Source File**:
A single CSV delivery of a Source Table. New Source Files arriving over time are what make ingestion incremental.
_Avoid_: extract, input file

### Bronze

**Bronze Layer**:
The raw landing layer: Source Table data stored as delivered, plus ingestion metadata, with no business cleansing.
_Avoid_: raw layer, staging, landing zone

**Schema Evolution**:
A change in a Source Table's columns between Source Files that the Bronze Layer absorbs without failing the load.
_Avoid_: schema drift (when meaning the handling rather than the change)

### Run tracking

**Run**:
One execution of the bronze ingestion job for one Source Table.
_Avoid_: job, execution, batch

**Run Log**:
The record of every Run, one row per Run, holding its outcome and the Watermark it reached.
_Avoid_: audit table, job log, watermark table

**Watermark**:
The high-water mark of the latest successful Run for a Source Table, marking which Source Files have already been ingested.
_Avoid_: checkpoint, bookmark, cursor

**Full Reload**:
A Run that deliberately ingests all current Source Files of a Source Table, ignoring the Watermark.
_Avoid_: backfill, reset
