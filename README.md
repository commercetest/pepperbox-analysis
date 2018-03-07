# CommerceTest PepperBox Analysis Tool

## What is this?


## Why?


## How?

```bash
> # Pre-process CSV files
> cd in/10
> head -1 results-loadgen.0.csv > combined.csv && for filename in $(ls results*.csv); do sed 1d $filename >> combined.csv; done
```

```bash
> npm run process
> npm run serve
```
