# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

import glob
import logging
import os

from ..utils import write_report

functions = ['cleaning']

def run(path_in, path_out, params):
    # Parameters
    logger = logging.getLogger(params['logger_name'] + '.' + params['step_name'])

    # Path to all outputs. Assumed to be the same for all steps.
    path_root = os.path.split(path_out)[0]

    # Clean
    saved_space = 0
    for step in params['steps']:
        # Cleaning parameters
        pattern = step.get('pattern', '*')
        max_size = step.get('max_size')
        # If no cleaning parameters is defined, only clean based on size
        if 'pattern' not in step and 'max_size' not in step:
            max_size = 20 * 1024 * 1024

        for f in glob.glob(os.path.join(path_root, step['step_name'], pattern)):
            fsize = os.path.getsize(f)
            if max_size is None or os.path.getsize(fsize) > max_size:
                logger.info(f'Removing {f}')
                os.remove(f)
                saved_space += fsize

    # Report
    logger.info('Report: Writing stats')
    write_report(os.path.join(path_out, params['step_name']+'_report'), {'saved_space': saved_space})
