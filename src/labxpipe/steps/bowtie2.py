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

from ..interfaces import if_exe_bowtie2
from ..interfaces import if_exe_samtools
from ..utils import get_fastqs_per_end
from ..utils import write_report

functions = ['genomic_aligning',
             'bowtie2']

def get_max_ram(num_processor):
    # Assume 8GB per core
    return int((8 * 1024 * 1024 * 1024) / (num_processor * 0.2))

def run(path_in, path_out, params):
    # Parameters
    logger = logging.getLogger(params['logger_name'] + '.' + params['step_name'])

    # Check compress_sam and index_bam aren't used together
    assert not (params.get('compress_sam', False) and params.get('index_bam', False)), 'Options compress_sam and index_bam incompatible'

    # Input
    fq_files = get_fastqs_per_end(path_in, params.get('paired'), params.get('fastq_exts'), params.get('read_regexs_in'))
    # Check input
    if len(fq_files) == 0:
        raise ValueError('No input FASTQ file found')

    # Bowtie2 suppl. parameters
    others = []
    if 'output_unfiltered' in params:
        path_unfiltered = params['output_unfiltered']
        if 'paired' in params and params['paired']:
            if len(fq_files) == 1:
                logger.warning('The "paired" flag is set but only one read was found.')
                option_prefix = 'un'
            else:
                option_prefix = 'un-conc'
        else:
            option_prefix = 'un'
        if path_unfiltered.endswith('.gz'):
            others.append(f'--{option_prefix}-gz')
        elif path_unfiltered.endswith('.lz4'):
            others.append(f'--{option_prefix}-lz4')
        else:
            others.append(f'--{option_prefix}')
        others.append(os.path.join(path_out, path_unfiltered))
    if 'options' in params:
        others.extend(params['options'])
    if len(fq_files) == 1:
        fq_files.append(None)

    # Executable
    if 'path_bowtie2' in params:
        bowtie2_exe = os.path.join(params['path_bowtie2'], 'bowtie2')
    else:
        bowtie2_exe = None

    # Version
    logger.info(f'Using Bowtie2 {if_exe_bowtie2.get_bowtie2_version(bowtie2_exe)}')

    # Align
    stdout, stderr = if_exe_bowtie2.bowtie2(fq_files[0],
                                            fq_files[1],
                                            quality_score         = params.get('quality_scores'),
                                            outfile               = os.path.join(path_out, params['output']),
                                            bwt_index             = os.path.join(params['path_bowtie2_index'], params['index']),
                                            num_processor         = str(params['num_processor']),
                                            compress_sam          = params.get('compress_sam', False),
                                            compress_sam_cmd      = params.get('compress_sam_cmd'),
                                            others                = others,
                                            exe                   = bowtie2_exe,
                                            return_std            = True,
                                            cwd                   = path_out,
                                            logger                = logger)

    # Create and index BAM file
    if params.get('create_bam', False):
        if_exe_samtools.create_bam(os.path.join(path_out, params['output']), exe=bowtie2_exe, logger=logger)
    elif params.get('index_bam', False):
        if_exe_samtools.create_bam(os.path.join(path_out, params['output']), sort=True, max_memory=get_max_ram(params['num_processor']), exe=bowtie2_exe, logger=logger)
        if_exe_samtools.create_bam_index(os.path.join(path_out, params['output'].replace('.sam', '.bam')), exe=bowtie2_exe, logger=logger)

    # Report
    logger.info('Report: Writing logs')
    with open(os.path.join(path_out, 'bowtie2_err.log'), 'wt') as f:
        f.write(stderr)
    with open(os.path.join(path_out, 'bowtie2_out.log'), 'wt') as f:
        f.write(stdout)
    logger.info('Report: Writing stats')
    report = if_exe_bowtie2.get_bowtie2_report(os.path.join(path_out, 'bowtie2_err.log'))
    write_report(os.path.join(path_out, params['step_name']+'_report'), report)
