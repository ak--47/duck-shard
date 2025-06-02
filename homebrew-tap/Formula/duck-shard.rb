class DuckShard < Formula
  desc "Fast, cross-platform CLI tool to convert Parquet, CSV, NDJSON, JSON with DuckDB"
  homepage "https://github.com/ak--47/duck-shard"
  url "https://raw.githubusercontent.com/ak--47/duck-shard/main/duck-shard.sh"
  version "main"
  sha256 "9e001f7d7d42474bd706deb10c6d8b31d9f0d7c551fa35a4dfbdd1df1243019c" # Will be set by GitHub Action!
  license "MIT"

  depends_on "duckdb"

  def install
    bin.install "duck-shard.sh" => "duck-shard"
  end

  test do
    assert_match "Usage:", shell_output("#{bin}/duck-shard -h")
  end
end
