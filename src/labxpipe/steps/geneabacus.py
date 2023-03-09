# -*- coding: utf-8 -*-

#
# Copyright © 2013 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

import logging
import os

import pyfnutils as pfu
import pyfnutils.parallel

from ..interfaces import if_exe_geneabacus
from ..interfaces import if_exe_bg2bw
from .. import parallel_helpers

functions = ['counting', 'geneabacus']


def run(path_in, path_out, params):
    # Parameters
    logger = logging.getLogger(params['logger_name'] + '.' + params['step_name'])

    # Reference features first
    features = [f for f in params['features'] if 'count_reference' not in f] + [f for f in params['features'] if 'count_reference' in f]

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
    if 'path_geneabacus' in params:
        geneabacus_exe = os.path.join(params['path_geneabacus'], 'geneabacus')
    else:
        geneabacus_exe = None

    # Version
    logger.info(f'Using GeneAbacus {if_exe_geneabacus.get_geneabacus_version(geneabacus_exe)}')

    jobs = []
    convert_jobs = []
    for path_input, output_suffix, input_type in inputs:
        for feature in features:
            # Features
            format_features = None
            if 'path_json' in feature:
                if not os.path.exists(feature['path_json']):
                    path_features = os.path.join(params['path_annots'], feature['path_json'])
                else:
                    path_features = feature['path_json']
            elif 'path_tab' in feature:
                if not os.path.exists(feature['path_tab']):
                    path_features = os.path.join(params['path_annots'], feature['path_tab'])
                else:
                    path_features = feature['path_tab']
                format_features = 'tab'
            else:
                raise ValueError('Missing path_json or path_tab')

            # Count multis
            if 'count_multis' in feature:
                count_multis = feature['count_multis']
            else:
                count_multis = [1, 2, 900]

            # Count reference
            if 'count_reference' in feature:
                with open(os.path.join(path_out, feature['count_reference'] + '.csv'), 'rt') as f:
                    f.readline()
                    tmp = f.readline().strip().split(',')
                    count_totals = []
                    for icm in range(len(count_multis)):
                        count_totals.append(tmp[2 + (2 * icm)])
            else:
                count_totals = None

            # Profiles
            if 'profile_formats' in feature:
                profile_formats = feature['profile_formats']
                profile_paths = [os.path.join(path_out, feature['name'] + output_suffix + parallel_helpers.format2ext(pf)) for pf in feature['profile_formats']]
            else:
                profile_formats = None
                profile_paths = None

            # Output SAM
            if 'output_sam' in feature and feature['output_sam']:
                path_sam_out = os.path.join(path_out, feature['name'] + output_suffix + '.sam')
            else:
                path_sam_out = None

            # Count job
            job = {
                'path_features'            : path_features,
                'format_features'          : format_features,
                'fon_name'                 : feature.get('fon_name'),
                'fon_chrom'                : feature.get('fon_chrom'),
                'fon_coords'               : feature.get('fon_coords'),
                'fon_strand'               : feature.get('fon_strand'),
                'path_report'              : os.path.join(path_out, feature['name'] + output_suffix + '_report.json'),
                'read_strand'              : params.get('r1_strand'),
                'paired'                   : params.get('paired'),
                'ignore_nh_tag'            : params.get('ignore_nh_tag'),
                'read_min_overlap'         : feature.get('read_min_overlap'),
                'count_path'               : os.path.join(path_out, feature['name'] + output_suffix + '.csv'),
                'count_multis'             : count_multis,
                'count_totals'             : count_totals,
                'profile_formats'          : profile_formats,
                'profile_paths'            : profile_paths,
                'profile_type'             : feature.get('profile_type'),
                'profile_multi'            : feature.get('profile_multi'),
                'profile_no_coord_mapping' : feature.get('profile_no_coord_mapping'),
                'path_sam_out'             : path_sam_out,
                'num_worker'               : str(min(params['num_processor'], 3)),
                'others'                   : params.get('options', []) + feature.get('options', []),
                'exe'                      : geneabacus_exe,
                'logger'                   : logger,
            }
            if input_type == 'bam':
                job['path_bam'] = path_input
            elif input_type == 'sam':
                job['path_sam'] = path_input
            jobs.append(job)

            # Convert job
            if feature.get('create_bigwig', False):
                if not os.path.exists(feature['path_genome']):
                    path_genome = os.path.join(params['path_annots'], feature['path_genome'])
                else:
                    path_genome = feature['path_genome']
                for pp in profile_paths:
                    if pp.endswith('.bedgraph'):
                        job = {
                            'path_input': pp,
                            'path_chrom_list': path_genome,
                            'path_outfile': pp[: -1 * len('bedgraph')] + 'bw',
                            'logger': logger,
                        }
                        convert_jobs.append(job)

    # Run job(s)
    r = pfu.parallel.run(if_exe_geneabacus.geneabacus, jobs, num_processor=max(1, params['num_processor'] // 3))
    if r == 130:
        raise KeyboardInterrupt
    if len(convert_jobs) > 0:
        r = pfu.parallel.run(if_exe_bg2bw.bg2bw, convert_jobs, num_processor=params['num_processor'])
        if r == 130:
            raise KeyboardInterrupt
