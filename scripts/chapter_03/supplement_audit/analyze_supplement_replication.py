#!/usr/bin/env python3
"""Analyze eight complete paired replication-cost blocks."""
import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument("results",type=Path)
    args=ap.parse_args()
    data=pd.read_csv(args.results/"runs.csv")
    if len(data)!=32 or not all((data[x]=='PASS').all() for x in ('duckdb_status','plan_status')) or not (data.collector_status=='OK').all():
        raise ValueError('Need 32 measured actions with correctness, plan and collector passing')
    if data.duplicated(['block','level','method']).any() or data.block.nunique()!=8:
        raise ValueError('Duplicate conditions or missing blocks')
    data['condition']=data['level']+'_'+data['method'].str[0]
    frame=data.pivot(index='block',columns='condition',values='wall_clock_duration_s').sort_index()
    if set(frame.columns)!={'low_B','low_E','high_B','high_E'} or frame.isna().any().any() or (frame<=0).any().any():
        raise ValueError('Incomplete or invalid paired blocks')
    frame['R_low']=frame.low_E/frame.low_B
    frame['R_high']=frame.high_E/frame.high_B
    frame['R_effect']=frame.R_high/frame.R_low
    frame.to_csv(args.results/'paired_block_ratios.csv')
    rng=np.random.default_rng(20261128)
    indexes=rng.integers(0,len(frame),size=(100000,len(frame)))
    rows=[]
    for name in ('R_low','R_high','R_effect'):
        x=frame[name].to_numpy()
        ci=np.quantile(np.median(x[indexes],axis=1),[0.025,0.975])
        rows.append({'contrast':name,'paired_blocks':len(x),'median_ratio':float(np.median(x)),
                     'ci95_low':float(ci[0]),'ci95_high':float(ci[1]),'bootstrap_resamples':100000,
                     'bootstrap_seed':20261128,'multiple_comparison_adjusted':False})
    pd.DataFrame(rows).to_csv(args.results/'paired_analysis.csv',index=False)
    print(json.dumps(rows,indent=2))


if __name__=='__main__':
    main()
