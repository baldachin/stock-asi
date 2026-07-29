from datetime import date
import os
import tempfile
import unittest

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import update_kdata_parquet as mod


class TdxSafeIncrementalWriterTest(unittest.TestCase):
    def test_merge_preserves_non_fetched_symbols_in_replaced_date_range(self):
        """TDX 全量表中，增量更新不能删掉未抓取的退市股/B股/指数。"""
        with tempfile.TemporaryDirectory(prefix="hermes-tdx-writer-test-") as tmp:
            path = os.path.join(tmp, "kdata.parquet")
            schema = pa.schema([
                ("id", pa.int64()), ("symbol", pa.string()), ("date", pa.date32()),
                ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()),
                ("close", pa.float64()), ("volume", pa.int64()), ("amount", pa.float64()),
            ])
            old = pd.DataFrame([
                [1, "600000", date(2026, 7, 27), 10.0, 11.0, 9.0, 10.0, 100, 1000.0],
                [2, "900901", date(2026, 7, 27), 1.0, 1.1, 0.9, 1.0, 200, 2000.0],
                [3, "600000", date(2026, 7, 28), 11.0, 12.0, 10.0, 11.0, 110, 1100.0],
                [4, "900901", date(2026, 7, 28), 1.1, 1.2, 1.0, 1.1, 210, 2100.0],
            ], columns=schema.names)
            pq.write_table(pa.Table.from_pandas(old, schema=schema, preserve_index=False), path)
            new = pd.DataFrame([
                [date(2026, 7, 27), "600000", 20.0, 21.0, 19.0, 20.0, 300, 3000.0],
                [date(2026, 7, 28), "600000", 21.0, 22.0, 20.0, 21.0, 310, 3100.0],
            ], columns=["date", "symbol", "open", "high", "low", "close", "volume", "amount"])

            real_path = mod.PARQUET_PATH
            try:
                mod.PARQUET_PATH = path
                mod.merge_and_write(new)
            finally:
                mod.PARQUET_PATH = real_path

            got = pq.read_table(path).to_pandas().sort_values(["date", "symbol"]).reset_index(drop=True)
            self.assertEqual(len(got), 4)
            self.assertEqual(set(zip(got.symbol, got.date)), {
                ("600000", date(2026, 7, 27)), ("900901", date(2026, 7, 27)),
                ("600000", date(2026, 7, 28)), ("900901", date(2026, 7, 28)),
            })
            self.assertEqual(got.loc[(got.symbol == "600000") & (got.date == date(2026, 7, 28)), "close"].item(), 21.0)
            self.assertEqual(got.loc[(got.symbol == "900901") & (got.date == date(2026, 7, 28)), "close"].item(), 1.1)


if __name__ == "__main__":
    unittest.main()
