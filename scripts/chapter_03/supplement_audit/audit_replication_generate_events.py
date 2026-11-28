#!/usr/bin/env python3
"""Trace measured Generate output-row SQL metrics in an event log."""
import argparse
import collections
import csv
import json
from pathlib import Path


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('results',type=Path)
    ap.add_argument('event_log',type=Path)
    args=ap.parse_args()
    rows=json.loads((args.results/'runs.json').read_text())
    groups={r['job_group'] for r in rows}
    group_sql=collections.defaultdict(set)
    nodes=collections.defaultdict(dict)
    values=collections.defaultdict(list)
    with args.event_log.open() as f:
        for line in f:
            e=json.loads(line); kind=e.get('Event','')
            if kind=='SparkListenerJobStart':
                props=e.get('Properties') or {}; group=props.get('spark.jobGroup.id')
                if group in groups: group_sql[group].add(props.get('spark.sql.execution.id'))
            elif kind.endswith('SparkListenerSQLAdaptiveExecutionUpdate'):
                execution=str(e['executionId'])
                def visit(node):
                    if 'Generate' in node.get('nodeName',''):
                        for metric in node.get('metrics',[]):
                            if metric.get('name')=='number of output rows': nodes[execution][metric['accumulatorId']]=node['nodeName']
                    for child in node.get('children',[]): visit(child)
                if e.get('sparkPlanInfo'): visit(e['sparkPlanInfo'])
            elif kind=='SparkListenerStageCompleted':
                for acc in e['Stage Info'].get('Accumulables',[]):
                    if acc.get('Value') is not None: values[acc['ID']].append((e['Stage Info']['Stage ID'],str(acc['Value'])))
    out=[]
    for r in rows:
        if not r['method'].startswith('E_'):continue
        ids=group_sql[r['job_group']]
        if len(ids)!=1: raise ValueError(f"Expected one execution for {r['job_group']}: {ids}")
        execution=next(iter(ids)); candidates=nodes[execution]
        if len(candidates)!=1: raise ValueError(f"Expected one Generate metric for execution {execution}: {candidates}")
        acc=next(iter(candidates)); updates=values[acc]
        if not updates: raise ValueError(f"No completed-stage value for accumulator {acc}")
        out.append({'block':r.get('block',1),'level':r['level'],'method':r['method'],'job_group':r['job_group'],
                    'sql_execution_id':execution,'accumulator_id':acc,'metric_final_value':updates[-1][1],
                    'contributing_stage_ids':','.join(str(x[0]) for x in updates)})
    dest=args.results/'generate_metric_audit.csv'
    with dest.open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=out[0].keys());w.writeheader();w.writerows(out)
    print('Audited',len(out),'Generate metrics:',dest)
    for row in out:print(row)


if __name__=='__main__':main()
