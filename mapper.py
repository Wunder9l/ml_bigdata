#!/usr/bin/env python
"""mapper.py"""

### Use the same script for mean value and for variance, because
### there is a need for mean in variance (there no sense to compute
### a mean value separately)

import sys
import csv

SEP = ','

def update_var(ci, mi, vi, cj, mj, vj):
    fp = (ci * vi + cj * vj) / (ci + cj)
    sp = (mi - mj) / (ci + cj)
    return fp + ci * cj * (sp * sp)


def update_mean(ci, mi, cj, mj):
    return (ci * mi + cj * mj) / (ci + cj)


count = 0
current_mean = 0.0
current_var = 0.0

# input comes from STDIN (standard input)
for line in csv.reader(sys.stdin, delimiter=SEP):
    price = float(line[9])
    current_var = update_var(count, current_mean, current_var, 1, price, 0)
    current_mean = update_mean(count, current_mean, 1, price)
    count += 1

print('{}{}{}{}{}'.format(count, SEP, current_mean, SEP, current_var))