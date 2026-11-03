#!/usr/bin/env python3
"""Regenerate Chapter 3 ND2/ND3 charts from official January Standalone summaries."""
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
BENCH = ROOT / 'data/benchmarks'

plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.titlesize':12,'axes.titleweight':'bold'})

# ND2: metrics from the exact official Standalone summary for Jan 2026.
df = pd.read_csv(BENCH/'benchmark_nd2_aggregation_summary_official_standalone.csv')
methods = ['Baseline_GroupBy','Selective_Salting_S8','Uniform_Salting_S8']
df = df.set_index('Phương án').loc[methods].reset_index()
labels=['Baseline\ngroupBy','Selective\nSalting S=8','Uniform\nSalting S=8']
colors=['#4e79a7','#f28e2b','#9c89b8']
fig,axes=plt.subplots(2,2,figsize=(11,7.2))
charts=[
 ('Thời gian trung vị (s)','Thời gian Median (s)',False),
 ('Shuffle Write trung vị (MB)','Shuffle Write Median (MB)',False),
 ('TSR trung vị','TSR Max Median',False),
 ('PSR trung vị','PSR Median',True),
]
for ax,(title,col,na_ok) in zip(axes.flat,charts):
 vals=pd.to_numeric(df[col].replace('N/A',np.nan),errors='coerce').to_numpy(dtype=float)
 bars=ax.bar(range(len(vals)),np.nan_to_num(vals,nan=0),color=colors,edgecolor='#333333',linewidth=.6)
 ax.set_title(title); ax.set_xticks(range(len(labels)),labels); ax.grid(axis='y',alpha=.25); ax.set_axisbelow(True)
 ymax=max([v for v in vals if np.isfinite(v)] or [1]); ax.set_ylim(0,ymax*1.22 if ymax else 1)
 for bar,v in zip(bars,vals):
  if np.isfinite(v):
   ax.text(bar.get_x()+bar.get_width()/2,bar.get_height()+max(ymax*.025,.01),f'{v:.2f}',ha='center',va='bottom',fontsize=9,fontweight='bold')
  elif na_ok:
   ax.text(bar.get_x()+bar.get_width()/2,max(ymax*.04,.03),'Không khả dụng',ha='center',va='bottom',fontsize=8,fontweight='bold')
fig.suptitle('ND2 — Benchmark official Standalone, Silver tháng 01/2026\nTrung vị của 3 lượt đo; nguồn: benchmark_nd2_aggregation_summary_official_standalone.csv',fontsize=12,fontweight='bold')
fig.tight_layout(rect=[0,0,1,.93]); fig.savefig(BENCH/'figure_nd2_aggregation_benchmark.png',dpi=180,bbox_inches='tight'); plt.close(fig)

# ND3: same official run tier and month; spill and shuffle use the recorded summary values.
df=pd.read_csv(BENCH/'benchmark_nd3_join_summary_official_standalone.csv')
methods=['Baseline_SMJ','Spark_AQE_Skew_Join','Salting_Join_S8']
df=df.set_index('Phương án').loc[methods].reset_index()
labels=['Baseline\nSort-Merge','Spark AQE\nSkew Join','Salting Join\nS=8']
colors=['#4e79a7','#59a14f','#e15759']
fig,axes=plt.subplots(2,2,figsize=(11,7.2))
series=[
 ('Thời gian trung vị (s)','Thời gian Median (s)'),
 ('TSR trung vị','TSR Max Median'),
 ('Shuffle Read trung vị (MB)','Shuffle Read Median (MB)'),
]
for ax,(title,col) in zip(axes.flat[:3],series):
 vals=pd.to_numeric(df[col],errors='coerce').to_numpy(dtype=float)
 bars=ax.bar(range(len(vals)),vals,color=colors,edgecolor='#333333',linewidth=.6)
 ax.set_title(title); ax.set_xticks(range(len(labels)),labels); ax.grid(axis='y',alpha=.25); ax.set_axisbelow(True); ax.set_ylim(0,max(vals)*1.22 if max(vals)>0 else 1)
 for bar,v in zip(bars,vals): ax.text(bar.get_x()+bar.get_width()/2,bar.get_height()+max(max(vals)*.025,.01),f'{v:.2f}',ha='center',va='bottom',fontsize=9,fontweight='bold')
ax=axes.flat[3]
mem=pd.to_numeric(df['Memory Spill Median (MB)'],errors='coerce').to_numpy(dtype=float)
disk=pd.to_numeric(df['Disk Spill Median (MB)'],errors='coerce').to_numpy(dtype=float)
x=np.arange(3); w=.34
b1=ax.bar(x-w/2,mem,w,label='Memory spill',color='#f28e2b',edgecolor='#333333',linewidth=.5)
b2=ax.bar(x+w/2,disk,w,label='Disk spill',color='#b07aa1',edgecolor='#333333',linewidth=.5)
ax.set_title('Spill trung vị (MB)'); ax.set_xticks(x,labels); ax.grid(axis='y',alpha=.25); ax.set_axisbelow(True); ax.legend(frameon=False,fontsize=8)
ax.set_ylim(0,max(max(mem),max(disk))*1.25 if max(max(mem),max(disk))>0 else 1)
for bars,vals in ((b1,mem),(b2,disk)):
 for bar,v in zip(bars,vals): ax.text(bar.get_x()+bar.get_width()/2,bar.get_height()+max(max(max(mem),max(disk))*.025,.01),f'{v:.2f}',ha='center',va='bottom',fontsize=8)
fig.suptitle('ND3 — Benchmark official Standalone, Silver tháng 01/2026\nTrung vị của 3 lượt đo; nguồn: benchmark_nd3_join_summary_official_standalone.csv',fontsize=12,fontweight='bold')
fig.tight_layout(rect=[0,0,1,.93]); fig.savefig(BENCH/'figure_nd3_join_benchmark.png',dpi=180,bbox_inches='tight'); plt.close(fig)
print('Updated figures from official Standalone summaries:')
print(BENCH/'figure_nd2_aggregation_benchmark.png')
print(BENCH/'figure_nd3_join_benchmark.png')
