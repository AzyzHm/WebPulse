Machine: 12 logical CPUs, Windows-11-10.0.26300-SP0, Python 3.12.10. 492 pages per run, mock latency 50-300 ms.

| Configuration | Runs | Time (s) mean ± std | Pages/s mean ± std | Speed-up |
|---|---:|---:|---:|---:|
| sequential | 2 | 106.10 ± 0.66 | 4.6 ± 0.0 | 1.0x |
| threaded 10 | 3 | 15.58 ± 0.27 | 31.6 ± 0.5 | 6.8x |
| threaded 50 | 3 | 10.38 ± 0.69 | 47.5 ± 3.1 | 10.2x |
| threaded 100 | 3 | 10.37 ± 1.29 | 47.9 ± 5.6 | 10.2x |
| asyncio sem 5 | 3 | 22.63 ± 0.40 | 21.7 ± 0.4 | 4.7x |
| asyncio sem 10 | 3 | 12.65 ± 0.12 | 38.9 ± 0.4 | 8.4x |
| asyncio sem 50 | 3 | 6.25 ± 0.54 | 79.1 ± 6.6 | 17.0x |
| asyncio sem 100 | 3 | 5.50 ± 0.07 | 89.5 ± 1.1 | 19.3x |
| hybrid 1 proc | 3 | 6.85 ± 0.31 | 71.9 ± 3.3 | 15.5x |
| hybrid 2 proc | 3 | 3.68 ± 0.64 | 136.0 ± 21.5 | 28.8x |
| hybrid 4 proc | 3 | 3.75 ± 0.13 | 131.4 ± 4.5 | 28.3x |
| hybrid 8 proc | 3 | 3.77 ± 0.18 | 130.7 ± 6.2 | 28.1x |
