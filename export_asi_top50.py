import pandas as pd
from datetime import datetime

asi = pd.read_parquet('~/stock_data/asi_yearly.parquet')
basic = pd.read_parquet('~/stock_data/stock_basic.parquet')

latest_year = asi['year'].max()
basic['上市日期'] = pd.to_datetime(basic['上市日期'])

# 上市超一年: 2025-05-11之前上市
eligible = basic[basic['上市日期'] <= '2025-05-11']['代码'].tolist()
print(f'上市超一年股票数: {len(eligible)}')

# 2026年ASI Top50 (上市满一年)
asi_latest = asi[(asi['year'] == latest_year) & (asi['symbol'].isin(eligible))]
top50 = asi_latest.nlargest(50, 'asi_sum')
top50 = top50.merge(basic[['代码','名称']], left_on='symbol', right_on='代码', how='left')

output = top50[['asi_yearly_rank','symbol','名称','asi_sum','asi_mean','asi_best_rank','asi_avg_rank','top50_sum','top100_sum','asi_trading_days']]
output.columns = ['排名','代码','名称','asi_sum','asi_mean','最佳排名','平均排名','top50天','top100天','交易天数']

csv_path = 'asi_top50_2026_上市满一年.csv'
output.to_csv(csv_path, index=False, encoding='utf-8-sig')
print(f'Saved: {csv_path}')
print(f'共 {len(output)} 条\n')
print(output.to_string(index=False))
