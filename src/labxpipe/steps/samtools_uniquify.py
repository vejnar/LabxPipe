# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

import logging
import os
import subprocess

from ..interfaces import if_exe_samtools
from ..utils import write_report

functions = ['samtools_uniquify']

def run(path_in, path_out, params):
    # Parameters
    logger = logging.getLogger(params['logger_name'] + '.' + params['step_name'])

    # samtools suppl. parameters
    others = []
    if 'options' in params:
        others.extend(params['options'])

    # Input path
    if 'input' in params:
        path_input_sam = os.path.join(path_in, params['input'])
    else:
        path_input_sam = os.path.join(path_in, 'accepted_hits.bam')
    # Keep top input path for stats
    path_top_input_sam = path_input_sam
    # Output path
    if 'output' in params:
        path_output_sam = os.path.join(path_out, params['output'])
    else:
        path_output_sam = os.path.join(path_out, 'accepted_hits_unique.bam')

    # Executable
    if 'path_samtools' in params:
        samtools_exe = os.path.join(params['path_samtools'], 'samtools')
    else:
        samtools_exe = 'samtools'

    # Version
    logger.info(f'Using samtools {if_exe_samtools.get_samtools_version(samtools_exe)}')

    # Prepare paired-end reads (using fixmate)
    if params.get('paired'):
        cmd = [samtools_exe,
               'fixmate',
               '-m',
               path_input_sam,
               os.path.join(path_out, 'accepted_hits_fixmate.bam')]
        logger.info('Starting samtools (fixing mate) with ' + str(cmd))
        subprocess.run(cmd, check=True)
        cmd = [samtools_exe,
               'sort',
               '-o', os.path.join(path_out, 'accepted_hits_fixmate_sort.bam'),
               os.path.join(path_out, 'accepted_hits_fixmate.bam')]
        logger.info('Starting samtools (position sort) with ' + str(cmd))
        subprocess.run(cmd, check=True)
        path_input_sam = os.path.join(path_out, 'accepted_hits_fixmate_sort.bam')

    # Start samtools markdup
    cmd = [samtools_exe,
           'markdup',
           '-r']
    cmd += others
    cmd += [path_input_sam,
            os.path.join(path_out, 'accepted_hits_st.bam')]
    logger.info('Starting samtools with ' + str(cmd))
    subprocess.run(cmd, check=True)

    # Re-sort by read name
    if params.get('sort_by_name_bam', False):
        cmd = [samtools_exe,
               'sort',
               '-n',
               '-o', path_output_sam,
               os.path.join(path_out, 'accepted_hits_st.bam')]
        logger.info('Starting samtools (read-name sort) with ' + str(cmd))
        subprocess.run(cmd, check=True)
    else:
        os.rename(os.path.join(path_out, 'accepted_hits_st.bam'), path_output_sam)

    # Index BAM file
    if params.get('index_bam', False):
        if_exe_samtools.create_bam_index(path_output_sam,
                                         exe = samtools_exe,
                                         logger = logger)

    # Compute report
    logger.info('Report')
    report = {}
    # Input
    raw_report = if_exe_samtools.sam_stats(path_top_input_sam,
                                           exe = samtools_exe,
                                           logger = logger)
    report['input'] = raw_report['reads mapped']
    # Output
    raw_report = if_exe_samtools.sam_stats(path_output_sam,
                                           exe = samtools_exe,
                                           logger = logger)
    report['output'] = raw_report['reads mapped']
    # Report
    write_report(os.path.join(path_out, params['step_name']+'_report'), report)
