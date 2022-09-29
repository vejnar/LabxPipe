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

from ..interfaces import if_exe_samtools
from ..interfaces import if_exe_star
from ..utils import get_fastqs_per_end
from ..utils import write_report

functions = ['aligning',
             'star']

def run(path_in, path_out, params):
    # Parameters
    logger = logging.getLogger(params['logger_name'] + '.' + params['step_name'])

    # STAR suppl. parameters
    others = []
    if 'options' in params:
        others.extend(params['options'])
        options = params['options']
    else:
        options = []

    # Executable
    if 'path_star' in params:
        star_exe = os.path.join(params['path_star'], 'STAR')
    else:
        star_exe = None

    # Version
    logger.info(f'Using STAR {if_exe_star.get_star_version(star_exe)}')

    # Input
    fq_files = get_fastqs_per_end(path_in, params.get('paired'), params.get('fastq_exts'), params.get('read_regexs_in'))
    # Is run really paired?
    if 'paired' in params and params['paired'] and len(fq_files) == 1:
        logger.warning('The "paired" flag is set but only one read was found.')

    # Check
    if len(fq_files) == 0:
        raise ValueError('No input FASTQ file found')
    elif len(fq_files) == 1:
        fq_files.append(None)

    # Align
    stdout, stderr = if_exe_star.star(fq_files[0],
                                      fq_files[1],
                                      quality_score           = params['quality_scores'],
                                      outpath                 = path_out + '/',
                                      reads_directional       = params['directional'],
                                      star_index              = os.path.join(params['path_star_index'], params['index']),
                                      num_processor           = str(params['num_processor']),
                                      output_type             = params.get('output_type'),
                                      rename                  = params.get('rename', True),
                                      compress_sam            = params.get('compress_sam', False),
                                      compress_unmapped       = params.get('compress_unmapped', True),
                                      compress_sam_cmd        = params.get('compress_sam_cmd'),
                                      compress_unmapped_cmd   = params.get('compress_unmapped_cmd'),
                                      others                  = others,
                                      exe                     = star_exe,
                                      return_std              = True,
                                      logger                  = logger)
    # Write output
    with open(os.path.join(path_out, 'star_err.log'), 'wt') as f:
        f.write(stderr)
    with open(os.path.join(path_out, 'star_out.log'), 'wt') as f:
        f.write(stdout)
    # Report
    report = if_exe_star.parse_star_report(os.path.join(path_out, 'Log.final.out'))

    # Index BAM file(s)
    sorted_bam = 'output_type' in params and 'None' not in params['output_type'] and 'Unsorted' not in params['output_type'] and 'SAM' not in params['output_type']
    if sorted_bam:
        if_exe_samtools.create_bam_index(os.path.join(path_out, 'accepted_hits.bam'), logger = logger)
    if os.path.exists(os.path.join(path_out, 'accepted_hits_sorted.bam')):
        if_exe_samtools.create_bam_index(os.path.join(path_out, 'accepted_hits_sorted.bam'), logger = logger)

    # Report
    logger.info('Report')
    write_report(os.path.join(path_out, params['step_name']+'_report'), report)
