"""Summarize raw Go benchmark runs without external dependencies."""
import csv
import pathlib
import re
import statistics
import sys
from collections import defaultdict

source = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else 'research/kvo/results/bench-final.txt')
groups = defaultdict(list)
metrics = defaultdict(list)
for line in source.read_text().splitlines():
    if not line.startswith('Benchmark'):
        continue
    columns = line.split()
    if 'ns/op' not in columns:
        continue
    def metric(unit):
        return float(columns[columns.index(unit)-1])
    groups[columns[0]].append((metric('ns/op'), metric('B/op'), metric('allocs/op')))
    for i in range(2, len(columns)-1, 2):
        try:
            metrics[(columns[0], columns[i+1])].append(float(columns[i]))
        except ValueError:
            continue
target = source.with_suffix('.csv')
with target.open('w', newline='') as f:
    writer = csv.writer(f, lineterminator="\n")
    writer.writerow(['benchmark', 'runs', 'median_ns', 'min_ns', 'max_ns', 'median_bytes', 'median_allocs'])
    for name, samples in groups.items():
        values = [s[0] for s in samples]
        writer.writerow([name,len(samples),statistics.median(values),min(values),max(values),statistics.median(s[1] for s in samples),statistics.median(s[2] for s in samples)])
print(f'{len(groups)} benchmark cases summarized to {target}')

# Preserve custom metrics (row/file bytes, copied bytes, alignment, etc.) too.
metric_target = source.with_suffix('.metrics.csv')
with metric_target.open('w', newline='') as f:
    writer = csv.writer(f, lineterminator="\n")
    writer.writerow(['benchmark', 'metric', 'runs', 'median', 'min', 'max'])
    for (name, unit), values in metrics.items():
        writer.writerow([name, unit, len(values), statistics.median(values), min(values), max(values)])
