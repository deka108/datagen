# Datagen


## Generate data for experimentations


### Distributed TPCH-data

Require: git clone git@github.com:lovasoa/TPCH-sqlite.git

1. Generate `TPC-H.db`
```bash
./tpch gen-tpch.sh [path to the TPCH-sqlite repo] [SCALE] # this generate data under tpch directory
```

2. Run `gen_dist_tpch.py` to distribute data in `TPC-H.db`

To be done:
- [x] integrate sqlite, pandas, and numpy for easy data generation to multiple node settings
- [x] distribute data based on tuple count distributions: `equal`, `left`, `right`, `random` 
- [ ] partition data into nodes based on table and columns (can use consisten hashing)
