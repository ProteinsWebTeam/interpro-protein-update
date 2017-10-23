#!/usr/bin/env bash

python setup_pyswiss.py clean --all
python setup_pyswiss.py install

python setup_mundone.py clean --all
python setup_mundone.py install
