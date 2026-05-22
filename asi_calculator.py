"""
ASI (Amount Strength Index) 成交额强度指标计算

核心逻辑:
- 每日按成交额排名，计算得分: ln(max_rank + 1 - rank) / ln(max_rank + 1) × 100
- 年度聚合: 累加得分(asi_sum)、日均分(asi_mean)、进入前50/100天数

输入: kdata.parquet (symbol, open, high, low, close, amount, volume, date)
输出: asi_yearly.parquet
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os

# ========== 配置 ==========
KDATA_PATH = '/home/hanshuang8902/stock/kdata.parquet'
OUTPUT_PATH = '/home/hanshuang8902/stock/asi_yearly.parquet'
TOP_N_LIST = [50, 100]  # 统计进入前N名的天数

# ========== 核心计算函数 ==========

def calc_daily_asi_score(amount_rank: int, max_rank: int) -> float:
    """
    计算单日ASI得分
    score = ln(max_rank + 1 - rank) / ln(max_rank + 1) × 100
    
    参数:
        amount_rank: 当日成交额排名 (从1开始)
        max_rank: 当日市场股票总数 (即排名最大值)
    返回:
        得分 (0-100)
    """
    if amount_rank > max_rank or amount_rank < 1:
        return 0.0
    score = np.log(max_rank + 1 - amount_rank) / np.log(max_rank + 1) * 100
    return round(score, 6)


def calculate_asi():
    """主计算流程"""
    print(f"[{datetime.now()}] 开始读取数据: {KDATA_PATH}")
    df = pd.read_parquet(KDATA_PATH, columns=['symbol', 'date', 'amount'])
    
    # 只保留有效成交额数据
    df = df[df['amount'] > 0].copy()
    print(f"数据量: {len(df):,} 行")
    print(f"日期范围: {df['date'].min().date()} ~ {df['date'].max().date()}")
    
    # 提取年份
    df['year'] = df['date'].dt.year
    
    print(f"[{datetime.now()}] 开始按日计算成交额排名和ASI得分...")
    
    # 按日分组计算排名
    daily_results = []
    dates = sorted(df['date'].unique())
    total_dates = len(dates)
    
    for i, date in enumerate(dates):
        if i % 500 == 0:
            print(f"  进度: {i}/{total_dates} ({100*i/total_dates:.1f}%)")
        
        day_df = df[df['date'] == date].copy()
        max_rank = len(day_df)  # 当日有成交的股票总数
        
        # 按成交额降序排名
        day_df['amount_rank'] = day_df['amount'].rank(method='min', ascending=False)
        
        # 计算ASI得分
        day_df['asi_score'] = day_df['amount_rank'].apply(
            lambda r: calc_daily_asi_score(int(r), max_rank)
        )
        
        # 标记是否进入top N
        for n in TOP_N_LIST:
            day_df[f'top{n}'] = (day_df['amount_rank'] <= n).astype(int)
        
        daily_results.append(day_df[['symbol', 'date', 'year', 'amount_rank', 'asi_score'] + 
                                    [f'top{n}' for n in TOP_N_LIST]])
    
    print(f"  进度: {total_dates}/{total_dates} (100.0%)")
    daily_df = pd.concat(daily_results, ignore_index=True)
    print(f"[{datetime.now()}] 每日得分计算完成，共 {len(daily_df):,} 行")
    
    # ========== 年度聚合 ==========
    print(f"[{datetime.now()}] 开始年度聚合...")
    
    # 基础聚合: sum, mean
    agg_dict = {
        'asi_score': ['sum', 'mean', 'std', 'count'],
        'amount_rank': ['min', 'mean'],  # min_rank越小越好
    }
    for n in TOP_N_LIST:
        agg_dict[f'top{n}'] = 'sum'  # 进入topN的天数
    
    yearly_df = daily_df.groupby(['symbol', 'year']).agg(agg_dict).reset_index()
    
    # 扁平化列名
    yearly_df.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) else col 
                          for col in yearly_df.columns]
    
    # 重命名列
    yearly_df = yearly_df.rename(columns={
        'asi_score_sum': 'asi_sum',
        'asi_score_mean': 'asi_mean',
        'asi_score_std': 'asi_std',
        'asi_score_count': 'asi_trading_days',
        'amount_rank_min': 'asi_best_rank',
        'amount_rank_mean': 'asi_avg_rank',
    })
    
    # 计算理论最大得分（用于归一化参考）
    # 理论满分 = sum_{r=1}^{max_rank} ln(max_rank + 1 - r) / ln(max_rank + 1) * 100
    # 这里用全年日均max_rank作为参考
    yearly_df['asi_max_theoretical'] = 100.0
    
    # 计算得分率 (实际得分 / 理论满分, 满分=100*年交易日数)
    yearly_df['asi_score_ratio'] = (yearly_df['asi_sum'] / 
                                     (yearly_df['asi_trading_days'] * 100)).round(6)
    
    # 排序
    yearly_df = yearly_df.sort_values(['year', 'asi_sum'], ascending=[True, False])
    yearly_df = yearly_df.reset_index(drop=True)
    
    # ========== 年度内排名 ==========
    # 按年度分组，对asi_sum进行排名
    yearly_df['asi_yearly_rank'] = yearly_df.groupby('year')['asi_sum'].rank(
        method='min', ascending=False).astype(int)
    
    print(f"[{datetime.now()}] 年度聚合完成，共 {len(yearly_df):,} 条记录")
    
    # ========== 保存结果 ==========
    print(f"[{datetime.now()}] 保存结果到: {OUTPUT_PATH}")
    yearly_df.to_parquet(OUTPUT_PATH, index=False)
    
    # 打印统计摘要
    print("\n========== ASI 年度统计摘要 ==========")
    for year in sorted(yearly_df['year'].unique())[-5:]:
        yr_df = yearly_df[yearly_df['year'] == year]
        print(f"\n{year}年:")
        print(f"  股票数: {len(yr_df)}, "
              f"asi_sum均值: {yr_df['asi_sum'].mean():.2f}, "
              f"asi_mean均值: {yr_df['asi_mean'].mean():.4f}")
        print(f"  top50天数 > 0 的股票数: {(yr_df[f'top{50}_sum'] > 0).sum()}, "
              f"top100天数 > 0 的股票数: {(yr_df[f'top{100}_sum'] > 0).sum()}")
    
    print(f"\n[{datetime.now()}] 完成! 输出文件: {OUTPUT_PATH}")
    
    return yearly_df


if __name__ == '__main__':
    result = calculate_asi()
    print(f"\n输出数据预览:")
    print(result.head(10))
    print(f"\n列名: {result.columns.tolist()}")
