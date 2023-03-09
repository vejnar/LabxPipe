# -*- coding: utf-8 -*-

#
# Copyright © 2023 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""Interface with the `bg2bw <https://github.com/cancerit/cgpBigWig>`_ executable program."""

import subprocess


def get_bg2bw_version(exe=None):
    # Defaults
    if exe is None:
        exe = 'bg2bw'
    # Get version
    p = subprocess.run([exe], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.stdout.strip()


def bg2bw(path_input, path_outfile, path_chrom_list, others=None, exe=None, return_std=None, logger=None):
    # Defaults
    if exe is None:
        exe = 'bg2bw'
    if logger is None:
        import logging as logger
    # ---------
    # bg2bw cmd & parameters
    cmd = [exe]
    cmd.append('--input')
    cmd.append(path_input)
    cmd.append('--outfile')
    cmd.append(path_outfile)
    cmd.append('--chromList')
    cmd.append(path_chrom_list)
    # Add remaining parameters
    if others is not None:
        cmd.extend(others)
    # Start
    logger.info('Starting bg2bw with ' + str(cmd))
    if return_std:
        try:
            p = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return p.stdout, p.stderr
        except Exception as e:
            logger.error('bg2bw failed: ' + e.stderr)
            raise
    else:
        subprocess.run(cmd, check=True)
