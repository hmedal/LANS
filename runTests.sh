#!/usr/bin/env bash

echo "STARTING test: CTU13_5_Sample.csv"
python masterscript.py "CTU13_5_Sample.csv"
echo "STARTING test: CTU13_5.csv"
python masterscript.py "CTU13_5.csv"
echo "STARTING test: 5.binetflow"
python masterscript.py "5.binetflow"
