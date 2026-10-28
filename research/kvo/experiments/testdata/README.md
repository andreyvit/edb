# Local replay input

`workload-shapes.json.gz` is an optional, Git-ignored input from the database
distribution study. It describes 189 anonymized row shapes: nesting, lengths,
scalar widths and key spacing, from which the experiments generate dummy values.

Keep the collected file locally to reproduce sample-dependent experiments. The
experiment binary embeds it when present, including in portable benchmark bundles.
Without it, the package still builds and sample-dependent tests/benchmark families
explicitly skip. Handwritten synthetic fixtures remain part of the source tree.
Restore the local file and rebuild to enable the sampled cases; the repository
does not download or regenerate collected data automatically.

Do not commit collected input or generated results. Reports, profiles, logs and
source snapshots live in the ignored `research/kvo/results/` directory.
