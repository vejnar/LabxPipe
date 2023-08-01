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

functions = ['macs3']


def run(path_in, path_out, params):
    # Parameters
    logger = logging.getLogger(params['logger_name'] + '.' + params['step_name'])
    exe = 'macs3'

    # Version
    p = subprocess.run([exe, '--version'], check=True, stdout=subprocess.PIPE, text=True)
    version = p.stdout.strip().split(' ')[1]
    logger.info(f'Using MACS3 {version}')

    # Prepare cmd
    cmd = [exe]
    # Add sub-command (default callpeak)
    cmd.append(params.get('subcmd', 'callpeak'))

    # Input
    if '-t' in params['options'] or '--treatment' in params['options']:
        # Look for treatment option
        if '-t' in params['options']:
            treatment_idx = params['options'].index('-t')
        if '--treatment' in params['options']:
            treatment_idx = params['options'].index('--treatment')
        # Add input path to treatment option
        params['options'][treatment_idx + 1] = os.path.join(path_in, params['options'][treatment_idx + 1])
    elif '-f' in params['options'] or '--format' in params['options']:
        raise NotImplementedError('--treatment required')

    # Add remaining parameters
    if 'options' in params:
        cmd.extend(params['options'])

    # Run MACS3
    logger.info(f'Starting MACS3 with {cmd}')
    try:
        p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, cwd=path_out)
    except Exception as e:
        logger.error('MACS3 failed: ' + e.stderr)
        raise

    # Log
    logger.info('Writing logs')
    with open(os.path.join(path_out, 'macs3_err.log'), 'wt') as f:
        f.write(p.stderr)
    with open(os.path.join(path_out, 'macs3_out.log'), 'wt') as f:
        f.write(p.stdout)
