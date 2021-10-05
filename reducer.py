#!/usr/bin/env python
"""reducer.py"""

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
    # print(line)
    new_count, new_mean, new_var = int(line[0]), float(line[1]), float(line[2])
    current_var = update_var(count, current_mean, current_var, new_count, new_mean, new_var)
    current_mean = update_mean(count, current_mean, new_count, new_mean)
    count += new_count

print('{}{}{}{}{}'.format(count, SEP, current_mean, SEP, current_var))