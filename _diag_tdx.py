"""诊断 TDX 解析性能 (仅解析,不做合并)"""
import os, time, re, sys
import pandas as pd

TDX_DIR = 'F:/Develops/Stock/data/tdx_export/day'
LOG = 'F:/Develops/stock-asi/_diag_tdx.log'

def log(msg):
    ts = time.strftime('%H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG, 'a', encoding='utf-8') as f:
        f.write(line + '\n')

t0 = time.time()
log("开始 TDX 诊断")
files = sorted([f for f in os.listdir(TDX_DIR) if f.endswith('.txt')])
log(f"找到 {len(files)} 个文件")

# 用批量方式: 每 500 个文件为一组
BATCH = 500
total_rows = 0
all_dfs = []
for batch_start in range(0, len(files), BATCH):
    batch = files[batch_start:batch_start+BATCH]
    rows = []
    for fn in batch:
        path = os.path.join(TDX_DIR, fn)
        m = re.match(r'([A-Z]+)#(\d+)\.txt', fn)
        if not m: continue
        symbol = m.group(2)
        try:
            with open(path, 'r', encoding='gb18030', errors='replace') as f:
                content = f.read()
            # 跳过前 2 行
            lines = content.split('\n')
            for line in lines[2:]:
                line = line.strip()
                if not line or line.startswith('#'): continue
                parts = line.split('\t')
                if len(parts) != 7: continue
                if not re.match(r'\d{4}/\d{2}/\d{2}', parts[0]): continue
                rows.append([symbol] + parts)
        except Exception as e:
            log(f"  {fn}: {e}")
    if rows:
        df = pd.DataFrame(rows, columns=['symbol','date','open','high','low','close','volume','amount'])
        all_dfs.append(df)
        total_rows += len(df)
    elapsed = time.time() - t0
    log(f"[{min(batch_start+BATCH, len(files)):>4}/{len(files)}] {elapsed:.0f}s | 累计 {total_rows:,} 行")

log(f"总耗时: {time.time()-t0:.0f}s, 总行数: {total_rows:,}")
log("=== 完成 ===")