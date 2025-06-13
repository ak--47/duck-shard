class DuckShard < Formula
  desc "Fast, cross-platform CLI tool to convert Parquet, CSV, NDJSON, JSON with DuckDB"
  homepage "https://github.com/ak--47/duck-shard"
  url "https://raw.githubusercontent.com/ak--47/duck-shard/main/duck-shard.sh"
  version "main"
  sha256 "c9fecc6fe36ff42461b44e07b6cccf5deffb8cf54ca681181ce7a39c5cc30ef7" # Will be set by GitHub Action!
  license "MIT"

  depends_on "duckdb"

  def install
    bin.install "duck-shard.sh" => "duck-shard"
  end

  test do
    assert_match "Usage:", shell_output("#{bin}/duck-shard -h")
  end
end
