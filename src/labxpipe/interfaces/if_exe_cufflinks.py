# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""Interface with the `Cufflinks <http://cole-trapnell-lab.github.io/cufflinks>`_ executable programs."""

import os
import re
import subprocess

def get_cufflinks_version(exe=None):
    # Defaults
    if exe is None:
        exe = 'cufflinks'
    # Get version
    p = subprocess.run([exe], check=False, stderr=subprocess.PIPE, text=True)
    return re.search(r'cufflinks v([\.\w]+)', p.stderr).group(1)

def cufflinks(path_sam, outpath=None, read_strand=None, path_features=None, verbose=None, num_processor=None, others=None, exe=None, return_std=None, logger=None):
    # Defaults
    if exe is None:
        exe = 'cufflinks'
    if logger is None:
        import logging as logger
    # ---------
    # Cufflinks cmd & parameters
    cmd = [exe, '--no-update-check']
    # Output
    if outpath is not None:
        cmd.append('--output-dir')
        cmd.append(outpath)
    # Library type
    if read_strand == '+':
        cmd.append('--library-type')
        cmd.append('fr-secondstrand')
    elif read_strand == '-':
        cmd.append('--library-type')
        cmd.append('fr-firststrand')
    else:
        cmd.append('--library-type')
        cmd.append('fr-unstranded')
    # Features
    if path_features is not None:
        cmd.append('--GTF')
        cmd.append(path_features)
    # Verbose
    if verbose is not False:
        cmd.append('--verbose')
    # Number of threads
    if num_processor is not None:
        cmd.append('--num-threads')
        cmd.append(str(num_processor))
    # Adding remaining parameters
    if others is not None:
        cmd.extend(others)
    # Path SAM input
    cmd.append(path_sam)
    # ---------
    # Starting Cufflinks
    logger.info('Starting Cufflinks with ' + str(cmd))
    if return_std:
        try:
            p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
            return p.stdout, p.stderr
        except Exception as e:
            logger.error('Cufflinks failed: ' + e.stderr)
            raise
    else:
        subprocess.run(cmd, check=True)
