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

from ..interfaces import if_exe_minimap2
from ..interfaces import if_exe_samtools
from ..utils import get_fastqs_per_end
from ..utils import write_report

functions = ['minimap2']


def get_max_ram(num_processor):
    # Assume 8GB per core
    return int((8 * 1024 * 1024 * 1024) / (num_processor * 0.2))


def run(path_in, path_out, params):
    # Parameters
    logger = logging.getLogger(params['logger_name'] + '.' + params['step_name'])

    # Keep output SAM if BAM is requested by user
    compress_output_cmd = params.get('compress_output_cmd')
    if params.get('compress_output', False) and compress_output_cmd is None:
        if params.get('create_bam', False) or params.get('index_bam', False):
            compress_output_cmd = ['zstd', '--keep', '-12']
        else:
            compress_output_cmd = ['zstd', '--rm', '-12']
        logger.info(f'Output compression using {compress_output_cmd}')

    # Input
    fq_files = get_fastqs_per_end(path_in, params.get('paired'), params.get('fastq_exts'), params.get('read_regexs_in'))
    # Check input
    if len(fq_files) == 0:
        raise ValueError('No input FASTQ file found')

    # Minimap2 suppl. parameters
    others = []
    if 'options' in params:
        others.extend(params['options'])

    # Executable
    if 'path_minimap2' in params:
        minimap2_exe = os.path.join(params['path_minimap2'], 'minimap2')
    else:
        minimap2_exe = None

    # Version
    logger.info(f'Using Minimap2 {if_exe_minimap2.get_minimap2_version(minimap2_exe)}')

    # Align
    stdout, stderr = if_exe_minimap2.minimap2(
        [f for fqfs in fq_files for f in fqfs],
        outfile=os.path.join(path_out, params['output']),
        index=os.path.join(params['path_minimap2_index'], params['index']),
        num_processor=str(params['num_processor']),
        compress_output=params.get('compress_output', False),
        compress_output_cmd=compress_output_cmd,
        others=others,
        exe=minimap2_exe,
        return_std=True,
        cwd=path_out,
        logger=logger,
    )

    # Create and index BAM file
    if params.get('create_bam', False):
        if_exe_samtools.create_bam(os.path.join(path_out, params['output']), logger=logger)
    elif params.get('index_bam', False):
        if_exe_samtools.create_bam(
            os.path.join(path_out, params['output']),
            sort=True,
            max_memory=get_max_ram(params['num_processor']),
            logger=logger,
        )
        if_exe_samtools.create_bam_index(
            os.path.join(path_out, params['output'].replace('.sam', '.bam')), logger=logger
        )

    # Logs
    logger.info('Report: Writing logs')
    with open(os.path.join(path_out, 'minimap2_err.log'), 'wt') as f:
        f.write(stderr)
    with open(os.path.join(path_out, 'minimap2_out.log'), 'wt') as f:
        f.write(stdout)

    # Compute report
    logger.info('Report')
    report = {}
    # Input
    report = if_exe_minimap2.get_minimap2_report(os.path.join(path_out, 'minimap2_err.log'))
    # Output: If output is SAM
    if '-a' in others:
        raw_report = if_exe_samtools.sam_stats(os.path.join(path_out, params['output']), logger=logger)
        report['output'] = raw_report['reads mapped']
    # Report
    logger.info('Report: Writing stats')
    write_report(os.path.join(path_out, params['step_name'] + '_report'), report)
