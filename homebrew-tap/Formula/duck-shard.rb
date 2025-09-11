class DuckShard < Formula
  desc "Fast, cross-platform CLI tool to convert Parquet, CSV, NDJSON, JSON with DuckDB"
  homepage "https://github.com/ak--47/duck-shard"
  url "https://raw.githubusercontent.com/ak--47/duck-shard/v1.1.0/duck-shard.sh"
  version "1.1.0"
  sha256 "2e37fe9185806c55b5ee4bfa90b57a9b1dad13a262a89864d2668d2025d09b58"
  license "MIT"

  depends_on "duckdb"
  depends_on "node", optional: true

  def install
    bin.install "duck-shard.sh" => "duck-shard"
  end

  test do
    assert_match "Usage:", shell_output("#{bin}/duck-shard -h")
  end
end
