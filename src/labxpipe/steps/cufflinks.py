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

from ..interfaces import if_exe_cufflinks

functions = ['cufflinks']

def run(path_in, path_out, params):
    # Parameters
    logger = logging.getLogger(params['logger_name'] + '.' + params['step_name'])

    # Input
    if 'inputs' in params:
        inputs = []
        for ipt in params['inputs']:
            if 'step' in ipt:
                path_input = os.path.join(params['path_analysis'], ipt['step'])
            else:
                path_input = path_in
            inputs.append((os.path.join(path_input, ipt['fname']), ipt.get('suffix', ''), ipt.get('type', 'bam')))
    else:
        inputs = [(os.path.join(path_in, 'accepted_hits.bam'), '', 'bam')]

    # Executable
    if 'path_cufflinks' in params:
        cufflinks_exe = os.path.join(params['path_cufflinks'], 'cufflinks')
    else:
        cufflinks_exe = None

    # Version
    logger.info(f'Using Cufflinks {if_exe_cufflinks.get_cufflinks_version(cufflinks_exe)}')

    for path_input, output_suffix, input_type in inputs:
        for feature in params['features']:
            # Features
            if 'path_gff3' in feature:
                if not os.path.exists(feature['path_gff3']):
                    path_features = os.path.join(params['path_annots'], feature['path_gff3'])
                else:
                    path_features = feature['path_gff3']
            else:
                raise ValueError('Missing path_gff3')

            # Count
            stdout, stderr = if_exe_cufflinks.cufflinks(path_input,
                                                        outpath           = path_out,
                                                        path_features     = path_features,
                                                        read_strand       = params.get('r1_strand'),
                                                        num_processor     = str(params['num_processor']),
                                                        others            = params.get('options'),
                                                        exe               = cufflinks_exe,
                                                        return_std        = True,
                                                        logger            = logger)

    # Report
    logger.info('Report: Writing logs')
    with open(os.path.join(path_out, 'cufflinks_err.log'), 'wt') as f:
        f.write(stderr)
    with open(os.path.join(path_out, 'cufflinks_out.log'), 'wt') as f:
        f.write(stdout)
