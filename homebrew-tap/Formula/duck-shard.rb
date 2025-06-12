class DuckShard < Formula
  desc "Fast, cross-platform CLI tool to convert Parquet, CSV, NDJSON, JSON with DuckDB"
  homepage "https://github.com/ak--47/duck-shard"
  url "https://raw.githubusercontent.com/ak--47/duck-shard/main/duck-shard.sh"
  version "main"
  sha256 "5dc2b038c63a5166fae2e4ce4bc03b8cf0c1e5f829267ca973e16d773d8e3d33" # Will be set by GitHub Action!
  license "MIT"

  depends_on "duckdb"

  def install
    bin.install "duck-shard.sh" => "duck-shard"
  end

  test do
    assert_match "Usage:", shell_output("#{bin}/duck-shard -h")
  end
end
