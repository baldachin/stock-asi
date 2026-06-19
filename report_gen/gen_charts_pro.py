#!/usr/bin/env python3
"""生成专业图表"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.font_manager as fm
from scipy import stats
import os

SC_FONT = ''
fm.fontManager.addfont(SC_FONT)
sc_prop = fm.FontProperties(fname=SC_FONT)
plt.rcParams['font.family'] = ['Noto Sans SC', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

STOCK_NAME = '兆易创新'
DATA_PATH = 'F:/Develops/stock-asi/report_gen/stock_兆易创新.csv'
IMG_DIR = 'F:/Develops/stock-asi/report_gen'

def load_data():
    df = pd.read_csv(DATA_PATH, parse_dates=['date'])
    df = df.sort_values('date').reset_index(drop=True)
    df['returns'] = df['close'].pct_change()
    df['nav'] = df['close'] / df['close'].iloc[0]
    df['cummax'] = df['nav'].cummax()
    df['drawdown'] = (df['nav'] - df['cummax']) / df['cummax']
    return df

def calc_metrics(df):
    close = df['close'].values
    ret = df['returns'].dropna().values
    nav = df['nav'].values
    dd = df['drawdown'].values

    n_days = len(df)
    total_return = nav[-1] - 1
    ann_return = (1 + total_return) ** (252 / n_days) - 1
    ann_vol = np.std(ret) * np.sqrt(252)
    max_dd = float(np.min(dd))
    var95 = float(np.percentile(ret, 5))
    cvar95 = float(ret[ret <= var95].mean())
    skew = float(stats.skew(ret))
    kurt = float(stats.kurtosis(ret))
    peak_idx = int(np.argmax(nav))
    trough_idx = int(np.argmin(dd))
    sharpe = (ann_return - 0.025) / ann_vol if ann_vol > 0 else 0

    return {
        'nav': nav, 'drawdown': dd, 'close': close, 'ret': ret,
        'dates': df['date'].values,
        'ann_return': ann_return, 'ann_vol': ann_vol,
        'max_dd': max_dd, 'var95': var95, 'cvar95': cvar95,
        'skew': skew, 'kurt': kurt,
        'peak_idx': peak_idx, 'trough_idx': trough_idx,
        'sharpe': sharpe,
        'start': str(df['date'].iloc[0].date()),
        'end': str(df['date'].iloc[-1].date()),
        'peak_date': str(df['date'].iloc[peak_idx].date()),
        'trough_date': str(df['date'].iloc[trough_idx].date()),
        'last_close': close[-1],
        'nav_peak': float(np.max(nav)),
        'total_return': total_return,
        'trading_days': n_days,
    }

def style_axis(ax, has_legend=False):
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#e0e0e0')
    ax.spines['bottom'].set_color('#e0e0e0')
    ax.grid(True, alpha=0.3, linestyle='--', color='#e0e0e0')
    ax.tick_params(colors='#5f6368')
    if has_legend:
        ax.legend(loc='upper right', framealpha=0.9)

# 1. 净值与回撤图
def gen_nav_dd_chart(df, m):
    # 近1年数据
    df_1y = df.tail(252).copy().reset_index(drop=True)
    df_1y['nav'] = df_1y['close'] / df_1y['close'].iloc[0]
    df_1y['cummax'] = df_1y['nav'].cummax()
    df_1y['drawdown'] = (df_1y['nav'] - df_1y['cummax']) / df_1y['cummax']

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True,
                                     gridspec_kw={'height_ratios': [2, 1]})
    fig.patch.set_facecolor('white')

    # 净值曲线
    dates = pd.to_datetime(df_1y['date'])
    ax1.fill_between(dates, 1, df_1y['nav'], alpha=0.2, color='#1a73e8')
    ax1.plot(dates, df_1y['nav'], color='#1a73e8', linewidth=1.5)
    ax1.axhline(1, color='gray', linestyle='--', alpha=0.5, linewidth=0.8)

    # 标注峰值
    peak_idx = df_1y['nav'].idxmax()
    peak_date = pd.to_datetime(df_1y.loc[peak_idx, 'date'])
    peak_val = df_1y.loc[peak_idx, 'nav']
    ax1.scatter([peak_date], [peak_val], color='#34a853', s=80, zorder=5, marker='^')
    ax1.annotate(f'Peak {peak_val:.2f}x ({peak_date.strftime("%Y-%m-%d")})',
                 xy=(peak_date, peak_val),
                 xytext=(10, 5), textcoords='offset points',
                 fontsize=10, color='#34a853', fontproperties=sc_prop)

    ax1.set_ylabel('Net Value', fontsize=12, fontproperties=sc_prop)
    ax1.set_title(f'{STOCK_NAME} Net Value & Drawdown (Last 252 Trading Days)', fontsize=14, fontweight='bold', fontproperties=sc_prop)
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.2f}x'))
    style_axis(ax1)

    # 回撤曲线
    ax2.fill_between(dates, df_1y['drawdown']*100, 0, alpha=0.3, color='#ea4335')
    ax2.plot(dates, df_1y['drawdown']*100, color='#ea4335', linewidth=1)
    trough_idx = df_1y['drawdown'].idxmin()
    trough_date = pd.to_datetime(df_1y.loc[trough_idx, 'date'])
    trough_val = df_1y.loc[trough_idx, 'drawdown'] * 100
    ax2.scatter([trough_date], [trough_val], color='#c5221f', s=80, zorder=5, marker='v')
    ax2.annotate(f'MaxDD {trough_val:.1f}%\n({trough_date.strftime("%Y-%m-%d")})',
                 xy=(trough_date, trough_val),
                 xytext=(10, -25), textcoords='offset points',
                 fontsize=9, color='#c5221f', fontproperties=sc_prop)

    ax2.set_ylabel('Drawdown %', fontsize=12, fontproperties=sc_prop)
    ax2.set_xlabel('Date', fontsize=12, fontproperties=sc_prop)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    style_axis(ax2)

    plt.tight_layout()
    out_path = os.path.join(IMG_DIR, 'nav_dd_chart.png')
    plt.savefig(out_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f'Generated: {out_path}')

# 2. 波动率分析图
def gen_vol_chart(df, m):
    df_1y = df.tail(252).copy().reset_index(drop=True)
    ret_1y = df_1y['close'].pct_change().dropna().values

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.patch.set_facecolor('white')

    # 左: 波动率箱线图
    windows = [20, 60, 120]
    vol_data = []
    for w in windows:
        vols = []
        for i in range(w, len(ret_1y) + 1):
            vols.append(np.std(ret_1y[i-w:i]) * np.sqrt(252) * 100)
        vol_data.append(vols)

    bp = ax1.boxplot(vol_data, tick_labels=[f'{w}D' for w in windows], patch_artist=True)
    colors_box = ['#4CAF50', '#2196F3', '#FF9800']
    for patch, color in zip(bp['boxes'], colors_box):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)

    ax1.axhline(m['ann_vol']*100, color='#c5221f', linestyle='--', linewidth=1.5,
                label=f'Ann.Vol {m["ann_vol"]*100:.1f}%')
    ax1.set_ylabel('Annualized Volatility %', fontsize=11, fontproperties=sc_prop)
    ax1.set_title('Volatility Distribution', fontsize=12, fontproperties=sc_prop)
    ax1.legend(loc='upper right', fontsize=9)
    style_axis(ax1)

    # 右: 收益率分布
    ax2.hist(ret_1y * 100, bins=50, density=True, alpha=0.6, color='#1a73e8', edgecolor='white')
    x = np.linspace(ret_1y.min()*100, ret_1y.max()*100, 200)
    pdf = stats.norm.pdf(x, ret_1y.mean()*100, ret_1y.std()*100)
    ax2.plot(x, pdf, '#ea4335', linewidth=2, label='Normal Dist.')
    ax2.axvline(m['var95']*100, color='#fbbc04', linestyle='--', linewidth=2,
                label=f"VaR 5%={m['var95']*100:.2f}%")

    ax2.set_xlabel('Daily Return %', fontsize=11, fontproperties=sc_prop)
    ax2.set_ylabel('Density', fontsize=11, fontproperties=sc_prop)
    ax2.set_title(f'Return Distribution (Sk={m["skew"]:.2f}, Ku={m["kurt"]:.2f})',
                  fontsize=12, fontproperties=sc_prop)
    ax2.legend(loc='upper right', fontsize=9)
    style_axis(ax2)

    plt.tight_layout()
    out_path = os.path.join(IMG_DIR, 'vol_analysis.png')
    plt.savefig(out_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f'Generated: {out_path}')

# 3. K线图
def gen_kline_chart(df):
    df_1y = df.tail(252).copy().reset_index(drop=True)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True,
                                     gridspec_kw={'height_ratios': [3, 1]})
    fig.patch.set_facecolor('white')

    # 价格走势
    ax1.plot(df_1y['date'], df_1y['close'], color='#1a73e8', linewidth=1.2)
    ax1.set_ylabel('Close Price', fontsize=11, fontproperties=sc_prop)
    ax1.set_title(f'{STOCK_NAME} K-Line (Last 252 Trading Days)', fontsize=14, fontweight='bold', fontproperties=sc_prop)
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'¥{x:.0f}'))
    style_axis(ax1)

    # 成交量
    colors = ['#ea4335' if df_1y['close'].iloc[i] >= df_1y['open'].iloc[i] else '#34a853'
              for i in range(len(df_1y))]
    ax2.bar(df_1y['date'], df_1y['volume'], color=colors, alpha=0.7, width=1)
    ax2.set_ylabel('Volume', fontsize=11, fontproperties=sc_prop)
    ax2.set_xlabel('Date', fontsize=11, fontproperties=sc_prop)
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1e6:.0f}M'))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    style_axis(ax2)

    plt.tight_layout()
    out_path = os.path.join(IMG_DIR, 'kline_chart.png')
    plt.savefig(out_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f'Generated: {out_path}')

if __name__ == '__main__':
    df = load_data()
    m = calc_metrics(df)

    print(f'\n Stock: {STOCK_NAME}')
    print(f' Return: {(m["total_return"]*100):.1f}%')
    print(f' Ann.Return: {m["ann_return"]*100:.1f}%')
    print(f' Ann.Vol: {m["ann_vol"]*100:.1f}%')
    print(f' MaxDD: {m["max_dd"]*100:.1f}%')
    print(f' VaR: {m["var95"]*100:.2f}%')
    print(f' Sharpe: {m["sharpe"]:.2f}')

    gen_nav_dd_chart(df, m)
    gen_vol_chart(df, m)
    gen_kline_chart(df)

    print('\nAll charts generated!')