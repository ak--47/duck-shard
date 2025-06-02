# 🦆 duck-shard 🗂️

## 🤨 wat.

**The ultimate "batch everything to everything" CLI for your data lake.**

Convert folders or files of **Parquet**, **CSV**, or **NDJSON** (even JSONL/JSON) into...
**NDJSON**, **CSV**, or **Parquet**.
*Deduplicate. Merge. Split into row-limited shards. Parallelize across all your CPU cores. Output wherever you want.*
Powered by DuckDB. Cross-platform, no Python, no JVM, no drama.

**Perfect for:**

* Analytics engineering and data science
* Batch ETL jobs
* Getting data OUT of your warehouse or data lake FAST

---

## 👔 tldr;

Convert *any* supported file (or whole directory) to *any* supported format, at speed, with zero dependencies beyond DuckDB.

**Just install [DuckDB](https://duckdb.org/), drop in `duck-shard.sh`, and go.**

Or use homebrew:

```bash
brew tap ak--47/duck-shard  # homebrew-tap
brew install duckdb
duck-shard ./myfolder/ -f csv -o ./output/
```

---

## 💻 CLI usage

```bash
./duck-shard.sh <input_path> [max_parallel_jobs] [options]
```

Where `input_path` is a single file (any supported type), a **directory** containing
`.parquet`, `.csv`, `.ndjson`, `.jsonl`, or `.json` files, or a **cloud storage path** (see below).

---

### 🔧 Options (partial list)

| Option             | Meaning                                            |
| ------------------ | -------------------------------------------------- |
| `-f ndjson`        | Output as NDJSON files (default)                   |
| `-f csv`           | Output as CSV                                      |
| `-f parquet`       | Output as Parquet (merge/rewrite)                  |
| `-c col1,col2`     | Only include certain columns                       |
| `--dedupe`         | Remove duplicates (across all or chosen columns)   |
| `-s`               | Merge everything into a single file                |
| `-s filename`      | ...and specify the name for merged output          |
| `-o output_dir`    | Directory to place per-file outputs                |
| `-r N`             | Split outputs with N rows per file                 |
| `--stringify`      | Coerce all columns to string (useful for CSV/JSON) |
| `--sql file.sql`   | Use a custom SQL SELECT for output (see below!)    |
| `--gcs-key KEY`    | Google Cloud Storage HMAC key (see cloud section)  |
| `--gcs-secret SEC` | Google Cloud Storage HMAC secret                   |
| `--s3-key KEY`     | AWS S3 access key                                  |
| `--s3-secret SEC`  | AWS S3 secret key                                  |
| `-h`               | Print help                                         |

---

## 📜 Using the --sql flag

Use the `--sql` flag to apply a **custom SQL transformation** to your data **before writing output**.

* Your SQL file is evaluated with the source data as a DuckDB view named `input_data`.
* Use any valid DuckDB `SELECT ... FROM input_data ...` query.
* Columns and types will match your input file.

**You can use the flag to:**

* Select and rename columns
* Filter rows (e.g. `WHERE`)
* Transform data (e.g. casting, string ops, math)
* Aggregate/group (e.g. `GROUP BY`, `COUNT(*)`, etc.)


---

### **Example SQL file**

Save as `my-query.sql`:

```sql
SELECT
  event,
  user_id,
  CAST(time AS VARCHAR) AS time_str
FROM input_data
WHERE event IS NOT NULL;
```

---

### **Example usage**

```bash
./duck-shard.sh ./data/part-1.parquet --sql ./my-query.sql -f csv -o ./out/
```

This writes a CSV with only the columns and rows you want, as defined by your SQL.

---

## 🌩️ Cloud Storage Support (GCS & S3)

duck-shard supports reading from and writing to **Google Cloud Storage (`gs://`)** and **Amazon S3 (`s3://`)** buckets, using [DuckDB's httpfs extension](https://duckdb.org/docs/extensions/httpfs.html).

### **Setup**

1. **Enable HMAC authentication for your cloud provider.**

2. Pass your key/secret to duck-shard:

   **Google Cloud Storage:**

   ```
   ./duck-shard.sh gs://my-bucket/folder/ -f csv --gcs-key <YOUR_KEY_ID> --gcs-secret <YOUR_SECRET>
   ```

   **Amazon S3:**

   ```
   ./duck-shard.sh s3://my-bucket/folder/ -f parquet --s3-key <AWS_KEY_ID> --s3-secret <AWS_SECRET>
   ```

3. Output can be to local disk or (if supported by DuckDB) to a cloud path.

**NOTE:** You need DuckDB v0.9+ with the `httpfs` extension. First time usage may run `INSTALL httpfs` and `LOAD httpfs` automatically.

---

## 🚀 Examples

**Convert a single Parquet to NDJSON:**

```bash
./duck-shard.sh ./data/part-1.parquet
```

**Convert a directory of CSV files to NDJSON:**

```bash
./duck-shard.sh ./testData/csv -f ndjson -o ./out/
```

**Convert a folder of NDJSON to a merged Parquet file:**

```bash
./duck-shard.sh ./testData/ndjson -s all.parquet -f parquet
```

**Deduplicate on just "event" and "user\_id":**

```bash
./duck-shard.sh ./testData/parquet -f csv -c event,user_id --dedupe -o ./deduped
```

**Split a file into shards of 5000 rows:**

```bash
./duck-shard.sh ./data/part-1.parquet -r 5000 -o ./shards/
```

**Convert a GCS bucket folder to local CSV files:**

```bash
./duck-shard.sh gs://my-bucket/data/ -f csv --gcs-key ... --gcs-secret ... -o ./out/
```

**Merge a folder of Parquet files on S3 into a single NDJSON:**

```bash
./duck-shard.sh s3://my-bucket/data/ -s all.ndjson -f ndjson --s3-key ... --s3-secret ...
```

**Coerce all output columns to string (useful for weird CSV/JSON typing issues):**

```bash
./duck-shard.sh ./testData/parquet -s all.csv -f csv --stringify
```

**Show help:**

```bash
./duck-shard.sh -h
```

---

## 🗝 Features

* 🚀 **Convert Parquet, CSV, NDJSON, JSONL, JSON** — from file, directory, or cloud (GCS/S3)
* 🔄 **To NDJSON, CSV, or Parquet** — your choice!
* 🧩 **Merge to single file** (`-s`/`--single-file`) or keep outputs per input
* 💾 **Custom output directory** with `-o`
* 🏎 **Parallel processing** (as many jobs as you want)
* 🦄 **Deduplication** (by all columns, or by a subset)
* ✂️ **Column selection** with `-c`
* 🪓 **Split by rows** (e.g. `-r 10000` gives you part-1-1.ndjson, part-1-2.ndjson, ...)
* 🌩️ **Cloud support** for GCS & S3 (see above)
* 🪄 **Stringify columns** (`--stringify`) for messy data
* 🦾 **Works on macOS & Linux** — BSD and GNU tools supported
* 🦆 **No Python, No Node, No JVM** — just DuckDB and bash

---

## 📦 Installation

1. **Install [DuckDB](https://duckdb.org/docs/installation/)** (must be on your `$PATH`).
2. Download [`duck-shard.sh`](./duck-shard.sh).
3. Make it executable:

   ```bash
   chmod +x duck-shard.sh
   ```
4. (Optional) Install Bats for testing:

   ```bash
   make install-deps
   ```

---

## 🏗️ Implementation Notes

* DuckDB's file extension magic means you don't need to specify format — just point to the right files!
* Supports `.parquet`, `.csv`, `.ndjson`, `.jsonl`, `.json` as input.
* Output file(s) auto-named to match input unless overridden.
* Output directory for per-file mode (`-o`), or current dir by default.
* Single-file mode (`-s`) incompatible with chunking (`-r`).
* Parallel conversion uses background processes — tested on macOS & Linux.
* **Cloud support:** httpfs extension loaded automatically for `gs://` and `s3://` URIs.
* **Stringify mode:** All output columns cast to string (for CSV/NDJSON edge cases).

---

## 🤷 why?

Because sometimes you just want to crack open a bucket of files and make them *useful* — without spinning up Spark or wrestling with Pandas. No one-liner should require a 2GB docker image.

---

## 🧪 Testing

Run with [`bats`](https://github.com/bats-core/bats-core) and provided sample data:

```bash
make test
```

All file types, modes, and options are tested. See [`tests/test.bats`](./tests/test.bats).

---

## 🪧 License

MIT — go wild. PRs, feedback, and wild data dreams welcome.
[Raise an issue or open a PR!](https://github.com/ak--47/duck-shard/issues)

---

**Happy sharding!** 🦆
