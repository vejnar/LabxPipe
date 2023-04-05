# -*- coding: utf-8 -*-

#
# Copyright © 2023 Charles E. Vejnar
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

functions = ['samtools_sort']


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
    # Check input exists
    if not os.path.exists(path_input_sam):
        raise FileNotFoundError(f'{path_input_sam} not found')
    # Output path
    if 'output' in params:
        path_output_sam = os.path.join(path_out, params['output'])
    else:
        path_output_sam = os.path.join(path_out, 'accepted_hits.bam')

    # Executable
    if 'path_samtools' in params:
        samtools_exe = os.path.join(params['path_samtools'], 'samtools')
    else:
        samtools_exe = 'samtools'

    # Version
    logger.info(f'Using samtools {if_exe_samtools.get_samtools_version(samtools_exe)}')

    # Decompress SAM if necessary
    if path_input_sam.endswith('.zst'):
        cmd = ['zstd', '--decompress', '--keep', path_input_sam, '--output-dir-flat', path_out]
        logger.info('Starting zstd with ' + str(cmd))
        subprocess.run(cmd, check=True)
        # Update input path
        path_input_sam = os.path.join(path_out, os.path.basename(path_input_sam[:-4]))

    # Prepare samtools command
    cmd = [samtools_exe, 'sort', '--threads', str(params['num_processor']), '-o', path_output_sam]

    # Sort by read name
    if params.get('sort_by_name_bam', False):
        cmd.append('-n')

    # Other parameters
    cmd += others

    # Create index if possible
    if (
        (path_output_sam.find('.bam') != -1 or path_output_sam.find('.cram') != -1)
        and '-n' not in cmd
        and '-t' not in cmd
    ):
        cmd.append('--write-index')

    # Add input
    cmd.append(path_input_sam)

    # Run
    logger.info('Starting samtools with ' + str(cmd))
    subprocess.run(cmd, check=True)

    # Compute report
    logger.info('Report')
    report = {}
    # Input
    raw_report = if_exe_samtools.sam_stats(path_input_sam, exe=samtools_exe, logger=logger)
    report['input'] = raw_report['reads mapped']
    # Output
    raw_report = if_exe_samtools.sam_stats(path_output_sam, exe=samtools_exe, logger=logger)
    report['output'] = raw_report['reads mapped']
    # Report
    write_report(os.path.join(path_out, params['step_name'] + '_report'), report)
