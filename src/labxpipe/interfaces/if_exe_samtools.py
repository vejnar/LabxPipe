# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""Interface with the `Samtools <https://www.htslib.org>`_ executable program."""

import os
import subprocess

def get_samtools_version(exe=None):
    # Defaults
    if exe is None:
        exe = 'samtools'
    # Get version
    p = subprocess.run([exe, 'version'], check=True, stdout=subprocess.PIPE, text=True)
    return p.stdout.split()[1]

def create_bam(sam_fname, bam_fname=None, sort=False, max_memory=None, num_processor=None, exe=None, logger=None):
    # Defaults
    if bam_fname is None:
        bam_fname = sam_fname[:sam_fname.rfind('.sam')] + '.bam'
    if exe is None:
        exe = 'samtools'
    if logger is None:
        import logging as logger

    # Prepare cmd
    if sort:
        cmd = [exe, 'sort', '-O', 'bam', '-T', bam_fname, '-o', bam_fname]
        if max_memory is not None:
            cmd.append('-m')
            cmd.append(str(max_memory))
    else:
        cmd = [exe, 'view', '-O', 'bam', '-o', bam_fname]
    if num_processor is not None:
            cmd.append('-@')
            cmd.append(str(num_processor))
    cmd.append(sam_fname)

    # Run cmd
    logger.info('Creating BAM file with ' + str(cmd))
    subprocess.run(cmd, check=True)

def create_bam_index(bam_fname, exe=None, logger=None):
    # Defaults
    if exe is None:
        exe = 'samtools'
    if logger is None:
        import logging as logger
    # Command
    cmd = [exe, 'index', bam_fname]
    logger.info('Indexing BAM file with ' + str(cmd))
    # Run
    subprocess.run(cmd, check=True)

def sam_stats(bam_fname, exe=None, logger=None):
    # Defaults
    if exe is None:
        exe = 'samtools'
    if logger is None:
        import logging as logger
    # Command
    cmd = [exe, 'stats', bam_fname]
    logger.info('Compute SAM statistics with ' + str(cmd))
    # Run
    p = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, text=True)
    # Parse
    report = {}
    for rec in [l.strip().split('\t') for l in p.stdout.split('\n') if l.startswith('SN')]:
        f = rec[1].strip(':')
        if '.' in rec[2]:
            report[f] = float(rec[2])
        else:
            report[f] = int(rec[2])
    return report
