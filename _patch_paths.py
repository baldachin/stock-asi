#!/usr/bin/env python3
"""
将仓库中所有脚本的硬编码路径 /home/hanshuang8902/ 替换为本地 F:/Develops/ 路径。
"""
import os
import re

REPO_DIR = r"F:\Develops\stock-asi"
DATA_DIR = r"F:/Develops/stock_data"
SCRIPT_DIR = r"F:/Develops/stock-asi"
HOME_DIR = "/home/hanshuang8902"

# 替换规则 (按顺序,前面的优先匹配)
REPLACEMENTS = [
    # 数据目录 (F:/Develops/stock_data → F:/Develops/stock_data)
    ("F:/Develops/stock_data", DATA_DIR),
    # 项目目录 (F:/Develops/stock-asi → F:/Develops/stock-asi)
    ("F:/Develops/stock-asi", SCRIPT_DIR),
    # 字体路径 (用空字符串消除,dashboard 用 plotly 不需要此字体)
    ("", ""),
    # /tmp 锁文件 (改到 stock_data 目录,fcntl 在 Windows 不可用,但至少路径存在)
    ("F:/Develops/stock_data/update_kdata_parquet.lock", "F:/Develops/stock_data/update_kdata_parquet.lock"),
]

# 仅替换 .py 文件,且排除 old_scripts 子目录(历史脚本不需要)
TARGET_GLOBS = ["*.py"]

def patch_file(path: str) -> int:
    """返回修改的行数"""
    with open(path, "r", encoding="utf-8") as f:
        src = f.read()
    orig = src
    for old, new in REPLACEMENTS:
        if old in src:
            src = src.replace(old, new)
    if src != orig:
        with open(path, "w", encoding="utf-8") as f:
            f.write(src)
        # 统计行数差 (粗略)
        return 1
    return 0

def main():
    total = 0
    for root, dirs, files in os.walk(REPO_DIR):
        # 跳过 .git / venv
        dirs[:] = [d for d in dirs if d not in (".git", "stock_asi_venv", "__pycache__", ".claude")]
        for fn in files:
            if not fn.endswith(".py"):
                continue
            full = os.path.join(root, fn)
            n = patch_file(full)
            if n:
                print(f"  patched: {os.path.relpath(full, REPO_DIR)}")
                total += n
    print(f"\n共修改 {total} 个文件")

if __name__ == "__main__":
    main()