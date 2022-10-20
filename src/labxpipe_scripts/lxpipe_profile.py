#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""Profile & count using GeneAbacus"""

import argparse
import json
import os
import re
import subprocess
import sys

import pyfnutils as pfu
import pyfnutils.parallel

from labxpipe.interfaces import if_exe_geneabacus
from labxpipe import parallel_helpers

def main(argv=None):
    if argv is None:
        argv = sys.argv
    # Started from wrapper?
    prog = os.path.basename(argv[0])
    if argv[1] == 'profile':
        job_cmd = argv[:2]
        argv_parser = argv[2:]
        prog += ' profile'
    else:
        job_cmd = argv[:1]
        argv_parser = argv[1:]
    # GeneAbacus help
    p = subprocess.run(['geneabacus', '-h'], check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    geneabacus_help_re = []
    for line in p.stdout.split('\n'):
        res = re.match('^\s+(-\w.*)', line)
        if res is None:
            geneabacus_help_re.append(line)
        else:
            geneabacus_help_re.append('  -' + res.group(1))
    geneabacus_help = '\n'.join(geneabacus_help_re)
    # Arguments parser
    parser = argparse.ArgumentParser(prog=prog, formatter_class=argparse.RawDescriptionHelpFormatter, description='Profile and count using GeneAbacus.', epilog=geneabacus_help)
    parser.add_argument('-e', '--path_schema', dest='path_schema', action='store', required=True, help='Path to schema input file')
    parser.add_argument('-n', '--bam_folder', dest='bam_folder', action='store', default='aligning', help='Folder of BAM(s) input file (comma separated)')
    parser.add_argument('-b', '--bam_fname', dest='bam_fname', action='store', default='accepted_hits.bam', help='Filename of BAM input file')
    parser.add_argument('--input_sam', dest='input_sam', action='store_true', help='Input is SAM')
    parser.add_argument('-r', '--path_root_bams', dest='path_root_bams', action='store', default='.', help='Root path to BAM(s) input file (comma separated)')
    parser.add_argument('-o', '--path_root_output', dest='path_root_output', action='store', default='.', help='Root path to output')
    parser.add_argument('-s', '--label_suffix', dest='label_suffix', action='store', default='', help='Label suffix (i.e. _plus or _unique)')
    parser.add_argument('-p', '--processor', dest='num_processor', action='store', type=int, default=1, help='Number of processor')
    parser.add_argument('--verbose', dest='verbose', action='store_true', help='Verbose')
    args, other_args = parser.parse_known_args(argv_parser)

    # Parse GeneAbacus arguments
    count_options = {}
    if args.verbose:
        count_options['verbose'] = True
    i = 0
    while i < len(other_args):
        arg = other_args[i].lstrip('-')
        if other_args[i].startswith('-') and (i + 1) != len(other_args) and (other_args[i+1] == '-' or not other_args[i+1].startswith('-')):
            count_options[arg] = other_args[i+1]
            i += 2
        else:
            count_options[arg] = True
            i += 1

    # Read schema
    schema = json.load(open(args.path_schema, 'rt'))

    # Prepare job(s)
    jobs = parallel_helpers.get_count_jobs(merging_schema   = schema['merging'],
                                           path_root_bams   = args.path_root_bams.split(','),
                                           path_root_output = args.path_root_output,
                                           bam_folder       = args.bam_folder.split(','),
                                           bam_fname        = args.bam_fname,
                                           label_suffix     = args.label_suffix,
                                           input_sam        = args.input_sam,
                                           count_options    = count_options,
                                           check            = True)

    # Run job(s)
    r = pfu.parallel.run(if_exe_geneabacus.geneabacus, jobs, num_processor=args.num_processor)

if __name__ == '__main__':
    sys.exit(main())
