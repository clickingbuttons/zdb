This was an attempt to build something similar to clickhouse before I knew about clickhouse.

There were 2 important lessons learned:
1. A block size that supports equal speed random and sequential access is ideal.
This way you get TWO indices for the price of one.
The sweet spot is ~64Kb on PCIE3 NVMEs.
2. Scripting languages are neat as query languages, BUT:
- Don't support accessing data in multithreaded ways
- Need extra metadata to take advantage of indices
