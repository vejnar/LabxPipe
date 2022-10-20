# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""Helper functions for parallel processing of runs."""

import os

def format2ext(pff):
    pff_split = pff.split('+')
    if len(pff_split) == 1:
        pf, pz = pff_split[0], None
    else:
        pf, pz = pff_split[0], pff_split[1]
    if pf == 'binary':
        ext = '.bin'
    elif pf =='bedgraph':
        ext = '.bedgraph'
    elif pf == 'csv':
        ext = '.csv'
    if pz == 'lz4':
        ext += '.lz4'
    return ext

def get_count_jobs(merging_schema,
                   path_root_bams = ['.'],
                   path_root_output = '.',
                   bam_folder = ['aligning'],
                   bam_fname = None,
                   label_suffix = '',
                   input_sam = False,
                   count_options = {},
                   check = False):
    jobs = []
    for run in merging_schema:
        # Init. job
        job = count_options.copy()
        if 'options' in run:
            job.update(run['options'])

        # Input: Path to BAM(s)
        if input_sam:
            key_input = 'path_sam'
        else:
            key_input = 'path_bam'
        job[key_input] = []
        for ref in run['refs']:
            for path_root_bam in path_root_bams:
                if bam_fname is not None:
                    p = os.path.join(path_root_bam, ref, *bam_folder, bam_fname)
                else:
                    if input_sam:
                        p = os.path.join(path_root_bam, ref+'.sam')
                    else:
                        p = os.path.join(path_root_bam, ref+'.bam')
                if os.path.exists(p):
                    job[key_input].append(p)
        if check:
            assert len(job[key_input]) > 0, f"No SAM/BAM found for {run['refs']}"

        # Output: Counts
        if 'count_path' not in job:
            job['count_path'] = os.path.join(path_root_output, run['name'] + label_suffix + '_counts.csv')
        # Output: Profiles
        if 'profile_formats' in count_options and 'profile_paths' not in count_options:
            if isinstance(job['profile_formats'], list):
                profile_formats = job['profile_formats']
            else:
                profile_formats = job['profile_formats'].split(',')
            job['profile_paths'] = [os.path.join(path_root_output, run['name'] + label_suffix + '_profiles' + format2ext(pff)) for pff in profile_formats]

        # Add job with BAM total file size
        jobs.append([job, sum([os.path.getsize(f) for f in job[key_input]])])

    # Sort job(s) by BAM size
    jobs.sort(key=lambda j: j[1], reverse=True)

    return [j[0] for j in jobs]
