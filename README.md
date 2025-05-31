# parquet-to

## 🤨 wat.

Convert a folder (or file) full of Parquet files to NDJSON, CSV, or Parquet — **using DuckDB**.

Fast, flexible, cross-platform, and designed for real-world batch/ETL jobs. Great for analytics engineering, data science, and just poking at mountains of Parquet data from the CLI. Batch deduplication? Yes. Parallel file conversion? You bet. Merge everything into one file? Why not?!?!

**No Python required. No JVM. No drama.**


## 👔 tldr;

Convert an entire directory of Parquet files (or a single file) to NDJSON, CSV, or Parquet — *fast*. Optionally deduplicate, subset columns, or merge everything into one giant file.

**You need [DuckDB](https://duckdb.org/) installed and on your \$PATH.**

---

### 💻 CLI usage

```bash
./parquet-to.sh <input_path> [max_parallel_jobs] [options]
```

Where `input_path` is a `.parquet` file, or a **directory** full of `.parquet` files.

#### Common options

| Option          | Meaning                                          |
| --------------- | ------------------------------------------------ |
| `-f csv`        | Output CSV files                                 |
| `-f ndjson`     | Output NDJSON files (default)                    |
| `-f parquet`    | Output Parquet files (rewrite/merge)             |
| `-c col1,col2`  | Only include certain columns                     |
| `--dedupe`      | Remove duplicates (across all or chosen columns) |
| `-s`            | Merge all into a single output file              |
| `-s filename`   | Merge all into a single file (specify name)      |
| `-o output_dir` | Write outputs to this directory (per-file mode)  |
| `-h`            | Print help                                       |

---

#### 🚀 Examples

**Convert a single file to NDJSON:**

```bash
./parquet-to.sh ./data/mydata.parquet
```

**Convert a folder to CSVs (in parallel):**

```bash
./parquet-to.sh ./data/ -f csv -o ./out/
```

**Merge a directory into a single NDJSON:**

```bash
./parquet-to.sh ./data/ -s merged.ndjson
```

**Merge a directory into a single Parquet, deduping on all columns:**

```bash
./parquet-to.sh ./data/ -s merged.parquet -f parquet --dedupe
```

**Convert a directory, dedupe on specific columns, write outputs to ./results:**

```bash
./parquet-to.sh ./data/ -f ndjson -c id,email --dedupe -o ./results
```

**Convert with custom parallelism:**

```bash
./parquet-to.sh ./data/ 4 -f csv -o ./csv_out
```

**Help:**

```bash
./parquet-to.sh -h
```

---

## 🗝 Features

* Convert one file or a whole folder of Parquet files
* NDJSON, CSV, or Parquet output (your choice)
* Merge all files into a single output, or convert each file individually
* Write outputs wherever you want (`-o ./my/output/dir`)
* Parallel conversion (auto-detects your CPU)
* Deduplicate by any column (or all columns)
* Works on macOS and Linux
* Tiny dependencies: just DuckDB and bash

---

## 📦 Installation

Just download [`parquet-to.sh`](./parquet-to.sh), `chmod +x parquet-to.sh`, and run it. No Node, no Python, no nonsense.

You **must** have [`duckdb`](https://duckdb.org/docs/installation/) installed and on your PATH.

You can also install it via `make`:

```bash
make install-deps
```

---

## 🏗️ Implementation notes

* This script runs DuckDB SQL queries under the hood. You can extend it!
* If you specify `--dedupe` and `--cols`, deduplication is performed on just those columns (rest are dropped).
* Outputs in per-file mode go to the same directory as the source Parquet unless you use `-o`.
* Output filenames match the input base name, with the appropriate extension.
* Parallelism: defaults to all your CPU cores, override with a number after the path (`./parquet-to.sh ./data 4 ...`).
* Tested with GNU and BSD tools (works on Mac and Linux).

---

## 🤷 why?

Because sometimes you just want to flatten a pile of Parquet files into something you can grep, `jq`, or upload to something else. And you want to do it *now*, without spinning up Spark or Python scripts.

---

## 🧪 Testing

Tests are run with [`bats`](https://github.com/bats-core/bats-core) and included test Parquet data. See [`tests/test.bats`](./tests/test.bats).

Or just run the tests:

```bash
make test
```

---

## 🪧 License

MIT — go wild. PRs welcome. Bugs? [File an issue](https://github.com/ak--47/duck-shard/issues) or PR.
