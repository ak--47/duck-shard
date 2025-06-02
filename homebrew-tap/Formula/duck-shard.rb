class DuckShard < Formula
  desc "Fast, cross-platform CLI tool to convert Parquet, CSV, NDJSON, JSON with DuckDB"
  homepage "https://github.com/ak--47/duck-shard"
  url "https://raw.githubusercontent.com/ak--47/duck-shard/main/duck-shard.sh"
  version "main"
  sha256 "88b68701095900c0625503c1148bb813c4884019a3ffa54de3e9ec4199b772be" # Will be set by GitHub Action!
  license "MIT"

  depends_on "duckdb"

  def install
    bin.install "duck-shard.sh" => "duck-shard"
  end

  test do
    assert_match "Usage:", shell_output("#{bin}/duck-shard -h")
  end
end
