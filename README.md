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

---

## 💻 CLI usage

```bash
./duck-shard.sh <input_path> [max_parallel_jobs] [options]
```

Where `input_path` is a single file (any supported type) or a **directory** containing
`.parquet`, `.csv`, `.ndjson`, `.jsonl`, or `.json` files.

---

### 🔧 Options (partial list)

| Option          | Meaning                                          |
| --------------- | ------------------------------------------------ |
| `-f ndjson`     | Output as NDJSON files (default)                 |
| `-f csv`        | Output as CSV                                    |
| `-f parquet`    | Output as Parquet (merge/rewrite)                |
| `-c col1,col2`  | Only include certain columns                     |
| `--dedupe`      | Remove duplicates (across all or chosen columns) |
| `-s`            | Merge everything into a single file              |
| `-s filename`   | ...and specify the name for merged output        |
| `-o output_dir` | Directory to place per-file outputs              |
| `-r N`          | Split outputs with N rows per file               |
| `-h`            | Print help                                       |

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

**Run with 4 parallel jobs:**

```bash
./duck-shard.sh ./testData/ndjson 4 -f csv -o ./csvs
```

**Show help:**

```bash
./duck-shard.sh -h
```

---

## 🗝 Features

* 🚀 **Convert Parquet, CSV, NDJSON, JSONL, JSON** — from file or whole folder
* 🔄 **To NDJSON, CSV, or Parquet** — your choice!
* 🧩 **Merge to single file** (`-s`/`--single-file`) or keep outputs per input
* 💾 **Custom output directory** with `-o`
* 🏎 **Parallel processing** (as many jobs as you want)
* 🦄 **Deduplication** (by all columns, or by a subset)
* ✂️ **Column selection** with `-c`
* 🪓 **Split by rows** (e.g. `-r 10000` gives you part-1-1.ndjson, part-1-2.ndjson, ...)
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
