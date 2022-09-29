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

from ..interfaces import if_exe_readknead
from ..utils import get_fastqs_per_end

functions = ['preparing',
             'readknead']

def get_idx_step(step, ops):
    idx = 0
    for s in ops:
        if s['name'] == step:
            return idx
        idx += 1
    return None

def run(path_in, path_out, params):
    # Parameters
    logger = logging.getLogger(params['logger_name'] + '.' + params['step_name'])

    # Operations
    for oread, adaptor in [('ops_r1', 'adaptor_3p'), ('ops_r2', 'adaptor_5p')]:
        if oread in params:
            # Add new name
            idx_rename = get_idx_step('rename', params[oread])
            if idx_rename is not None and 'new_name' not in params[oread][idx_rename]:
                params[oread][idx_rename]['new_name'] = params['seq_ref'] + '.'
            # Add adaptor sequence
            idx_trim = get_idx_step('trim', params[oread])
            if idx_trim is not None and 'sequence' not in params[oread][idx_trim] and 'sequences' not in params[oread][idx_trim] and adaptor in params:
                params[oread][idx_trim]['sequence'] = params[adaptor]

    # Executable
    if 'path_readknead' in params:
        readknead_exe = os.path.join(params['path_readknead'], 'readknead')
    else:
        readknead_exe = None

    # Version
    logger.info(f'Using ReadKnead {if_exe_readknead.get_readknead_version(readknead_exe)}')

    # Parameters: input
    fq_files = get_fastqs_per_end(path_in, params.get('paired'), params.get('fastq_exts'), params.get('read_regexs_in'))
    # Check input
    if len(fq_files) == 0:
        raise ValueError('No input FASTQ file found')
    # Parameters: output
    fq_fname_out_r1 = os.path.basename(fq_files[0][0])
    if 'paired' in params and params['paired']:
        if len(fq_files) == 1:
            logger.warning('The "paired" flag is set but only one read was found.')
            fq_fname_out_r2 = None
        else:
            fq_fname_out_r2 = os.path.basename(fq_files[1][0])
    else:
        fq_files.append(None)
        fq_fname_out_r2 = None

    # Parameters: command input and output path(s)
    if fq_fname_out_r1.endswith('.gz'):
        fq_command_in = 'zcat'
        fq_fname_out_r1 = fq_fname_out_r1[:-3]
        if fq_fname_out_r2 is not None:
            fq_fname_out_r2 = fq_fname_out_r2[:-3]
    elif fq_fname_out_r1.endswith('.lz4'):
        fq_command_in = 'lz4cat'
        fq_fname_out_r1 = fq_fname_out_r1[:-4]
        if fq_fname_out_r2 is not None:
            fq_fname_out_r2 = fq_fname_out_r2[:-4]
    elif fq_fname_out_r1.endswith('.zst'):
        fq_command_in = 'zstdcat'
        fq_fname_out_r1 = fq_fname_out_r1[:-4]
        if fq_fname_out_r2 is not None:
            fq_fname_out_r2 = fq_fname_out_r2[:-4]
    else:
        fq_command_in = None

    # Parameters: command output and output path(s)
    fq_command_out = None
    if params.get('fastq_out', True):
        fq_path_out = path_out
        if 'zip_fastq_out' in params and params['zip_fastq_out'] is not None:
            if params['zip_fastq_out'] == 'gzip':
                fq_command_out = 'gzip,-'
                fq_fname_out_r1 += '.gz'
                if fq_fname_out_r2 is not None:
                    fq_fname_out_r2 += '.gz'
            elif params['zip_fastq_out'] == 'lz4':
                fq_command_out = 'lz4,-'
                fq_fname_out_r1 += '.lz4'
                if fq_fname_out_r2 is not None:
                    fq_fname_out_r2 += '.lz4'
            elif params['zip_fastq_out'] == 'zst':
                fq_command_out = 'zstd,-,-o'
                fq_fname_out_r1 += '.zst'
                if fq_fname_out_r2 is not None:
                    fq_fname_out_r2 += '.zst'
    else:
        fq_path_out = None
        fq_fname_out_r1 = None
        fq_fname_out_r2 = None

    # Parameters: stats
    if params.get('plot_fastq_in'):
        stats_in_path = os.path.join(path_out, 'stats_in')
    else:
        stats_in_path = None
    if params.get('plot_fastq'):
        stats_out_path = os.path.join(path_out, 'stats_out')
    else:
        stats_out_path = None

    # Run
    stdout, stderr = if_exe_readknead.readknead(fq_files[0],
                                                fq_files[1],
                                                fq_path_out,
                                                fq_fname_out_r1      = fq_fname_out_r1,
                                                fq_fname_out_r2      = fq_fname_out_r2,
                                                fq_command_in        = fq_command_in,
                                                fq_command_out       = fq_command_out,
                                                quality_score        = params['quality_scores'],
                                                ops_r1               = params.get('ops_r1'),
                                                ops_r2               = params.get('ops_r2'),
                                                report_path          = os.path.join(path_out, params['step_name']+'_report.json'),
                                                label                = params['label_short'],
                                                stats_in_path        = stats_in_path,
                                                stats_out_path       = stats_out_path,
                                                max_read_length      = params.get('max_read_length'),
                                                others               = params.get('options'),
                                                exe                  = readknead_exe,
                                                num_worker           = str(params['num_processor']),
                                                return_std           = True,
                                                logger               = logger)

    # Write output
    with open(os.path.join(path_out, 'readknead_err.log'), 'w') as f:
        f.write(stderr)
    with open(os.path.join(path_out, 'readknead_out.log'), 'w') as f:
        f.write(stdout)
