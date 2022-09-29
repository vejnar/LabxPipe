# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

import json
import os
import re

def parse_fastq_filename(fname, regex=r'.+_([R,I][1,2,3])\.f'):
    m = re.match(regex, fname)
    if m:
        return m.group(1)
    else:
        return None

def get_fastqs_per_end(path_seq, paired=False, fastq_exts=None, read_regexs=None):
    if fastq_exts is None:
        fastq_exts = ['.fastq']
    if read_regexs is None:
        if paired:
            read_regexs = [r'.+_(R1)(\.f|_)', r'.+_(R2)(\.f|_)']
        else:
            read_regexs = [r'.+_(R1)(\.f|_)']
    fastqs = [[] for i in range(len(read_regexs))]
    for path, dirs, files in os.walk(path_seq, followlinks=True):
        for fname in files:
            if any([fname.endswith(e) for e in fastq_exts]):
                for i, read_regex in enumerate(read_regexs):
                    m = parse_fastq_filename(fname, read_regex)
                    if m is not None:
                        fastqs[i].append(os.path.join(path, fname))
    return fastqs

def write_report(fname, report):
    json.dump(report, open(fname+'.json', 'w'), sort_keys=True, indent=4, separators=(',', ': '))
