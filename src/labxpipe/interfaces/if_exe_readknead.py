# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""Interface with the `ReadKnead <https://git.sr.ht/~vejnar/ReadKnead>`_ executable program."""

import json
import os
import subprocess

readknead_quality_scores = {'Solexa':['--ascii_min', '59', '--max_quality', '46'], 'Illumina 1.3':['--ascii_min', '64'], 'Illumina 1.5':['--ascii_min', '64'], 'Illumina 1.8':['--ascii_min', '33']}

def get_readknead_version(exe=None):
    # Defaults
    if exe is None:
        exe = 'readknead'
    # Get version
    p = subprocess.run([exe, '--version'], check=True, stdout=subprocess.PIPE, text=True)
    return p.stdout.strip()

def readknead(fq_1, fq_2=None, outpath=None, fq_fname_out_r1=None, fq_fname_out_r2=None, fq_command_in=None, fq_command_out=None, quality_score=None, ops_r1=None, ops_r2=None, report_path=None, label=None, num_worker=None, stats_in_path=None, stats_out_path=None, max_read_length=None, max_quality=None, ascii_min=None, verbose=None, verbose_level=None, others=None, exe=None, return_std=None, logger=None):
    # Defaults
    if exe is None:
        exe = 'readknead'
    if logger is None:
        import logging as logger
    # ---------
    # ReadKnead cmd & parameters
    cmd = [exe]
    # Fastq
    cmd.extend(['--fq_fnames_r1', ','.join(fq_1)])
    if fq_2 is not None:
        cmd.extend(['--fq_fnames_r2', ','.join(fq_2)])
    # Output
    if outpath is None:
        outpath = os.path.dirname(fq_1[0])
    else:
        cmd.extend(['--fq_path_out', outpath])
    # Output FASTQ name
    if fq_fname_out_r1 is not None:
        cmd.extend(['--fq_fname_out_r1', fq_fname_out_r1])
    if fq_fname_out_r2 is not None:
        cmd.extend(['--fq_fname_out_r2', fq_fname_out_r2])
    # Input FASTQ command
    if fq_command_in is not None:
        cmd.extend(['--fq_command_in', fq_command_in])
    else:
        # Add zcat if necessary
        if fq_1[0].endswith('.gz'):
            cmd.extend(['--fq_command_in', 'zcat'])
        # Add lz4 if necessary
        elif fq_1[0].endswith('.lz4'):
            cmd.extend(['--fq_command_in', 'lz4cat'])
        # Add zstd if necessary
        elif fq_1[0].endswith('.zst'):
            cmd.extend(['--fq_command_in', 'zstdcat'])
    # Output FASTQ command
    if fq_command_out is not None:
        cmd.extend(['--fq_command_out', fq_command_out])
    # Quality scores
    if quality_score is not None:
        if quality_score in readknead_quality_scores:
            cmd.extend(readknead_quality_scores[quality_score])
        else:
            cmd.extend(quality_score)
    else:
        # Max quality
        if max_quality:
            cmd.extend(['--max_quality', str(max_quality)])
        # Min ASCII
        if ascii_min:
            cmd.extend(['--ascii_min', str(ascii_min)])
    # Operations
    if ops_r1 is not None:
        if isinstance(ops_r1, str):
            cmd.extend(['--ops_r1', ops_r1])
        else:
            cmd.extend(['--ops_r1', json.dumps(ops_r1)])
    if ops_r2 is not None:
        if isinstance(ops_r2, str):
            cmd.extend(['--ops_r2', ops_r2])
        else:
            cmd.extend(['--ops_r2', json.dumps(ops_r2)])
    # Report
    if report_path is not None:
        cmd.extend(['--report_path', report_path])
    # Label
    if label is not None:
        cmd.extend(['--label', label])
    # Processor
    if num_worker is not None:
        cmd.extend(['--num_worker', str(num_worker)])
    # Stats
    if stats_in_path is not None:
        cmd.extend(['--stats_in_path', stats_in_path])
    if stats_out_path is not None:
        cmd.extend(['--stats_out_path', stats_out_path])
    # Max read length
    if max_read_length:
        cmd.extend(['--max_read_length', str(max_read_length)])
    # Verbose
    if verbose:
        cmd.append('--verbose')
    if verbose_level:
        cmd.extend(['--verbose_level', str(verbose_level)])
    # Add remaining parameters
    if others is not None:
        cmd.extend(others)
    # Start
    logger.info('Starting ReadKnead with ' + str(cmd))
    if return_std:
        try:
            p = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return p.stdout, p.stderr
        except Exception as e:
            logger.error('ReadKnead failed: ' + e.stderr)
            raise
    else:
        subprocess.run(cmd, check=True)
