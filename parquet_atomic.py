"""
Parquet 原子写入 helper

提供 write_atomic(table, path) - 写到 .tmp 文件，fsync，原子 rename
避免 writer crash 时留下半写文件

为什么需要这个:
- pyarrow pq.write_table 内部会用临时文件但不一定 fsync
- 直接覆盖原文件有损坏风险
- os.replace() 在 POSIX (Linux) 上是原子操作, 单文件系统下永不半成功

用法:
    import pyarrow as pa
    from parquet_atomic import write_atomic

    table = pa.table({"date": [...], "amount": [...]})
    write_atomic(table, "/path/to/data.parquet")
"""

import os
import pyarrow.parquet as pq


def write_atomic(table, target_path: str, compression: str = "snappy",
                 row_group_size: int = 1_000_000, **kwargs) -> None:
    """原子写入 Parquet 文件

    1. 写到 {target_path}.tmp
    2. fsync 强制刷盘 (防止 power loss 后文件内容为 0)
    3. os.replace 原子 rename (POSIX 保证)

    Args:
        table: pyarrow.Table
        target_path: 最终文件路径
        compression: 默认 snappy (速度/压缩比平衡)
        row_group_size: 默认 1M 行/row group (适合 pyarrow dataset 过滤)
    """
    tmp_path = target_path + ".tmp"

    # 1. 写临时文件 (会覆盖已存在的 .tmp)
    pq.write_table(
        table, tmp_path,
        compression=compression,
        row_group_size=row_group_size,
        use_dictionary=True,
        **kwargs
    )

    # 2. fsync - 强制 OS 把 page cache 刷到磁盘
    #    没有 fsync, kernel panic / power loss 可能让文件内容为 0
    fd = os.open(tmp_path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

    # 3. 原子 rename - POSIX 单文件系统下永不半成功
    os.replace(tmp_path, target_path)
