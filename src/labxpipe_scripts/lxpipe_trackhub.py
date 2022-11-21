#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""Create trackhub"""

import argparse
import copy
import json
import logging
import os
import re
import subprocess
import sys

import labxdb

import pyfnutils as pfu
import pyfnutils.parallel

from labxpipe.interfaces import if_exe_geneabacus
from labxpipe import parallel_helpers
from labxpipe import trackhub

def get_available_refs(path_root_bams, bam_folder, bam_names):
    refs = []
    for path_root_bam in path_root_bams:
        for d in os.listdir(path_root_bam):
            for bn in bam_names:
                p = os.path.join(path_root_bam, d, *bam_folder, bn)
                if os.path.exists(p):
                    refs.append(d)
    return refs

def altget(d, a, b=None, default=None):
    if a in d and d[a] is not None:
        return d[a]
    elif b in d and d[b] is not None:
        return d[b]
    else:
        return default

def make_config(projects=[], samples=[], replicates=[], runs=[], levels=['sample'], deduplicate_merging=True, path_trackhub_config=None, save_config=True, default_color='Blue3', dbl=None, path_root_bams=[], bam_folder='.', bam_names=[], logger=None):
    # Empty config
    config = {'tracks':[], 'merging': []}

    # Available data
    avail_refs = get_available_refs(path_root_bams, bam_folder, bam_names)
    assert len(avail_refs) > 0, 'No input data found. Please check input options (--path_root_bams, --bam_folder, --bam_names etc)'

    # Query DB
    result = []
    for search, refs in [('0 project_ref', projects), ('1 sample_ref', samples), ('2 replicate_ref', replicates), ('3 run_ref', runs)]:
        if len(refs) > 0:
            result += dbl.post('tree', {'search_criterion':[search+' EQUAL '+r for r in refs], 'search_gate':'OR', 'sort_criterion':['1 track_priority ASC', '2 replicate_order ASC', '3 run_order ASC'], 'limit':'ALL'})

    # Project
    if len(result) > 0:
        config['project'] = {'project_name': result[0]['project_name'], 'label_short': result[0]['label_short'], 'label_long': result[0]['label_long']}
    else:
        raise ValueError('No run/replicate/sample were found')

    merges = []
    tracks = []
    max_priority = 1

    for project in result:
        for sample in project['children']:
            # Track priority
            track_priority = sample['track_priority']
            if track_priority is None:
                track_priority = max_priority
                max_priority += 1
            else:
                max_priority += track_priority

            merge_sample_refs = []
            tmp_merges = []
            tmp_tracks = []
            for replicate in sample['children']:
                merge_replicate_refs = []

                # Infos
                paired = replicate['children'][0]['paired']
                r1_strand = replicate['children'][0]['r1_strand']

                # Replicates
                if replicate['replicate_ref'] in avail_refs:
                    merge_replicate_refs.append(replicate['replicate_ref'])

                # Runs
                for run in replicate['children']:
                    if run['run_ref'] in avail_refs:
                        merge_replicate_refs.append(run['run_ref'])

                # Add to merges
                if 'replicate' in levels:
                    label_short = replicate['label_short']
                    if 'sample' in levels and label_short == sample['label_short']:
                        label_short += ' R' + str(replicate['replicate_order'])
                    tmp_merges.append({'refs':merge_replicate_refs, 'label_short':label_short, 'label_long':altget(replicate, 'label_long', 'label_short'), 'level':'replicate', 'name':replicate['replicate_ref'], 'options':{'read_strand':r1_strand, 'paired':paired}})
                    tmp_tracks.append({'name':replicate['replicate_ref'], 'data':replicate['replicate_ref'], 'label_short':replicate['label_short'], 'label_long':altget(replicate, 'label_long', 'label_short'), 'track_priority':track_priority, 'track_color':altget(sample, 'track_color', default=default_color), 'level':'replicate', 'species':sample['species']})
                merge_sample_refs.extend(merge_replicate_refs)
            if 'sample' in levels:
                # Add replicate(s)
                if not deduplicate_merging or not (len(tmp_merges) == 1 and set(merge_sample_refs) == set(tmp_merges[0]['refs'])):
                    # Replicates are different than samples
                    merges.extend(tmp_merges)
                    tracks.extend(tmp_tracks)
                else:
                    # Single replicate with same data as sample: data is changed to sample in track and added, no data record added
                    for t in tmp_tracks:
                        t['data'] = sample['sample_ref']
                        tracks.append(t)
                # Add sample
                merges.append({'name':sample['sample_ref'], 'refs':merge_sample_refs, 'label_short':sample['label_short'], 'label_long':altget(sample, 'label_long', 'label_short'), 'level':'sample', 'options':{'read_strand':r1_strand, 'paired':paired}})
                tracks.append({'name':sample['sample_ref'], 'data':sample['sample_ref'], 'label_short':sample['label_short'], 'label_long':altget(sample, 'label_long', 'label_short'), 'track_priority':track_priority, 'track_color':altget(sample, 'track_color', default=default_color), 'level':'sample', 'species':sample['species']})
            else:
                merges.extend(tmp_merges)
                tracks.extend(tmp_tracks)

    # Save merges and tracks into config
    config['merging'] = merges
    config['tracks'] = tracks

    # Write config
    if save_config:
        if path_trackhub_config is None:
            path_trackhub_config = config['project']['project_name']+'.json'
        json.dump(config, open(path_trackhub_config, 'wt'), sort_keys=True, indent=2)

    return config, path_trackhub_config

def convert_bedgraph(path_bedgraph, path_genome, path_bigwig):
    subprocess.run(['wigToBigWig', path_bedgraph, path_genome, path_bigwig], check=True)

def make_bigwig(trackhub_config, path_root_bams, bam_folder, bam_names, input_sam, ignore_nh_tag, path_genome, path_features, path_mapping=None, strands=['combined', 'plus', 'minus'], profile_type='all-slice', profile_norm=False, profile_multi=None, profile_untemplated=None, profile_no_untemplated=None, profile_extension_length=None, profile_position_fraction=None, read_min_mapping_quality=None, read_in_proper_pair=None, fragment_min_length=None, fragment_max_length=None, path_root_output='.', delete_bedgraph=False, no_count=False, export_binary=False, update=False, num_processor=1, verbose=False, logger=None):
    # Prepare jobs
    jobs = []
    for bam_fname in bam_names:
        for strand in strands:
            count_options = {'profile_formats': ['bedgraph'],
                             'profile_norm': profile_norm,
                             'profile_no_coord_mapping': True,
                             'profile_type': profile_type,
                             'profile_multi': profile_multi,
                             'profile_untemplated': profile_untemplated,
                             'profile_no_untemplated': profile_no_untemplated,
                             'profile_extension_length': profile_extension_length,
                             'profile_position_fraction': profile_position_fraction,
                             'path_features': path_features,
                             'format_features': 'tab',
                             'path_mapping': path_mapping,
                             'read_min_mapping_quality': read_min_mapping_quality,
                             'read_in_proper_pair': read_in_proper_pair,
                             'fragment_min_length': fragment_min_length,
                             'fragment_max_length': fragment_max_length,
                             'verbose': verbose}
            merging_schema = copy.deepcopy(trackhub_config['merging'])
            label_suffix = ''
            if strand == 'combined':
                for merger in merging_schema:
                    merger['options']['read_strand'] = 'u'
            else:
                label_suffix += '_' + strand
                if strand == 'plus':
                    count_options['feature_strand'] = '1'
                if strand == 'minus':
                    count_options['feature_strand'] = '-1'
            if ignore_nh_tag:
                count_options['ignore_nh_tag'] = True
            if no_count:
                count_options['count_path'] = ''
            if export_binary:
                count_options['profile_formats'].append('binary+lz4')

            job = parallel_helpers.get_count_jobs(merging_schema   = merging_schema,
                                                  path_root_bams   = path_root_bams,
                                                  path_root_output = path_root_output,
                                                  bam_folder       = bam_folder,
                                                  bam_fname        = bam_fname,
                                                  label_suffix     = label_suffix,
                                                  input_sam        = input_sam,
                                                  count_options    = count_options,
                                                  check            = True)

            if update:
                for j in job:
                    if not all([os.path.exists(p.replace('_profiles.bedgraph', '.bw')) for p in j['profile_paths']]):
                        jobs.append(j)
            else:
                jobs.extend(job)

    # Run jobs
    pfu.parallel.run(if_exe_geneabacus.geneabacus, jobs, num_processor=num_processor)
    # Export to Bigwig
    convert_jobs = []
    path_bedgraphs = []
    for job in jobs:
        path_bedgraph = [p for p in job['profile_paths'] if p.endswith('.bedgraph')][0]
        path_bedgraphs.append(path_bedgraph)
        if os.path.getsize(path_bedgraph) > 0:
            convert_jobs.append([path_bedgraph, path_genome, path_bedgraph.replace('_profiles.bedgraph', '.bw')])
    pfu.parallel.run(convert_bedgraph, convert_jobs, num_processor=num_processor)
    # Clean
    if delete_bedgraph:
        for path_bedgraph in path_bedgraphs:
            os.remove(path_bedgraph)

def main(argv=None):
    if argv is None:
        argv = sys.argv
    # Started from wrapper?
    prog = os.path.basename(argv[0])
    if argv[1] == 'trackhub':
        job_cmd = argv[:2]
        argv_parser = argv[2:]
        prog += ' trackhub'
    else:
        job_cmd = argv[:1]
        argv_parser = argv[1:]
    # Parse arguments
    parser = argparse.ArgumentParser(prog=prog, description='Generate track hub.')
    # Main
    group = parser.add_argument_group('Main')
    group.add_argument('-j', '--projects', dest='projects', action='store', help='Projects (comma separated)')
    group.add_argument('-l', '--samples', dest='samples', action='store', help='Samples (comma separated)')
    group.add_argument('-n', '--replicates', dest='replicates', action='store', help='Replicates (comma separated)')
    group.add_argument('-r', '--runs', dest='runs', action='store', help='Runs (comma separated)')
    group.add_argument('-s', '--strands', dest='strands', action='store', default='combined,plus,minus', help='Strand \'combined\', \'plus\', \'minus\' (comma separated)')
    group.add_argument('-a', '--path_root_bams', dest='path_root_bams', action='store', default='.', help='Path to BAM(s) input file (comma separated)')
    group.add_argument('-i', '--path_trackhub_config', dest='path_trackhub_config', action='store', help='Path to config file')
    group.add_argument('--bam_folder', dest='bam_folder', action='store', default='aligning', help='Folder of BAM(s) input file (comma separated)')
    group.add_argument('--bam_names', dest='bam_names', action='store', default='accepted_hits.bam', help='Filename(s) of BAM input file (comma separated)')
    group.add_argument('-f', '--bam_suffixes', dest='bam_suffixes', action='store', default='', help='Prefix(s) of BAM input file (comma separated)')
    group.add_argument('--input_sam', dest='input_sam', action='store_true', help='Input is SAM')
    group.add_argument('-g', '--path_genome', dest='path_genome', action='store', help='Path to tabulated genome input file')
    group.add_argument('-u', '--species_ucsc', dest='species_ucsc', action='store', help='UCSC species name')
    group.add_argument('-m', '--levels', dest='levels', action='store', default='sample,replicate', help='Level to include (comma separated in sample, replicate)')
    group.add_argument('-d', '--delete_bedgraph', dest='delete_bedgraph', action='store_true', help='Delete bedgraph')
    group.add_argument('--no_count', dest='no_count', action='store_true', help='Don\'t generate count output')
    group.add_argument('-w', '--update', dest='update', action='store_true', help='Update')
    group.add_argument('-p', '--processor', dest='num_processor', action='store', type=int, default=1, help='Number of processor')
    group.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Verbose')
    group.add_argument('--path_config', dest='path_config', action='store', help='Path to config')
    group.add_argument('--http_url', '--labxdb_http_url', dest='labxdb_http_url', action='store', help='Database HTTP URL')
    group.add_argument('--http_login', '--labxdb_http_login', dest='labxdb_http_login', action='store', help='Database HTTP login')
    group.add_argument('--http_password', '--labxdb_http_password', dest='labxdb_http_password', action='store', help='Database HTTP password')
    group.add_argument('--http_path', '--labxdb_http_path', dest='labxdb_http_path', action='store', help='Database HTTP path')
    group.add_argument('--http_db', '--labxdb_http_db', dest='labxdb_http_db', action='store', help='Database HTTP DB')
    # GeneAbacus
    group = parser.add_argument_group('GeneAbacus')
    group.add_argument('--ignore_nh_tag', dest='ignore_nh_tag', action='store_true', help='Ignore NH SAM tag and consider all alignment unique')
    group.add_argument('--profile_type', dest='profile_type', action='store', default='all-slice', help='Profile type: \'first\', \'last\', \'position\', \'all\', \'all-extension\' or \'all-slice\'')
    group.add_argument('--profile_norm', dest='profile_norm', action='store_true', help='Normalize profile counts with total reads')
    group.add_argument('--profile_multi', dest='profile_multi', action='store', help='Maximum alignment multiplicity to include a read in profile')
    group.add_argument('--profile_untemplated', dest='profile_untemplated', action='store', help='Remove max untemplated nucleotide')
    group.add_argument('--profile_no_untemplated', dest='profile_no_untemplated', action='store_true', help='Include only read w/o untemplated nucleotide in profile')
    group.add_argument('--profile_extension_length', dest='profile_extension_length', action='store', help='Extension length for extension profile')
    group.add_argument('--profile_position_fraction', dest='profile_position_fraction', action='store', help='Fraction of position between start and end for position profile')
    group.add_argument('--read_min_mapping_quality', dest='read_min_mapping_quality', action='store', help='Minimum read mapping quality')
    group.add_argument('--read_in_proper_pair', dest='read_in_proper_pair', action='store_true', help='Only read in proper pair (default: all pairs)')
    group.add_argument('--fragment_min_length', dest='fragment_min_length', action='store', help='Minimum fragment length')
    group.add_argument('--fragment_max_length', dest='fragment_max_length', action='store', help='Maximum fragment length')
    group.add_argument('--export_binary', dest='export_binary', action='store_true', help='Export to binary')
    group.add_argument('--path_features', dest='path_features', action='store', help='Path to path_features (tabulated file)')
    group.add_argument('--path_mapping', dest='path_mapping', action='store', help='Path to feature name(s) mapping (tabulated file)')
    # Step
    group = parser.add_argument_group('Step')
    group.add_argument('-c', '--make_config', dest='make_config', action='store_true', help='Make Config file')
    group.add_argument('-t', '--make_trackhub', dest='make_trackhub', action='store_true', help='Make Track hub')
    group.add_argument('-b', '--make_bigwig', dest='make_bigwig', action='store_true', help='Make BigWig')
    args = parser.parse_args(argv_parser)

    # Get config (JSON single file or all files in path_config)
    config = {}
    paths = []
    if args.path_config is None:
        if 'HTS_CONFIG_PATH' in os.environ:
            paths.append(os.environ['HTS_CONFIG_PATH'])
        elif 'XDG_CONFIG_HOME' in os.environ:
            paths.append(os.path.join(os.environ['XDG_CONFIG_HOME'], 'hts'))
    else:
        paths.append(args.path_config)
    for path in paths:
        if os.path.isdir(path):
            for f in sorted(os.listdir(path)):
                if f.endswith('.json'):
                    config = {**config, **json.load(open(os.path.join(path, f)))}
        elif os.path.isfile(path):
            config = {**config, **json.load(open(path))}

    # Input local config from args
    vargs = vars(args)
    for a, v in vargs.items():
        if v is not None and (a not in config or v != parser.get_default(a)):
            config[a] = v
        if a in ['projects', 'samples', 'replicates', 'runs', 'strands', 'path_root_bams', 'bam_folder', 'bam_names', 'bam_suffixes', 'levels']:
            if v is None:
                config[a] = []
            else:
                config[a] = [r.strip() for r in v.split(',')]

    # Verbose
    if config['verbose']:
        logging.basicConfig(level='INFO')
        logger = logging.getLogger('gen_trackhub')
    else:
        logger = None

    # Check user inputs
    if config['make_config'] or config['make_bigwig']:
        assert 'species_ucsc' in config, 'No UCSC species name. Please specify one using --species_ucsc'
    if config['make_bigwig']:
        assert 'path_genome' in config, 'No path to genome. Please specify one using --path_genome'
        assert 'path_mapping' in config, 'No path to feature mapping. Please specify one using --path_mapping'

    # Make config
    if config['make_config']:
        if 'labxdb_http_path' not in config and 'labxdb_http_db' not in config:
            if 'labxdb_http_path_seq' in config:
                config['labxdb_http_path'] = config['labxdb_http_path_seq']
            else:
                config['labxdb_http_db'] = 'seq'
        dbl = labxdb.DBLink(config.get('labxdb_http_url'), config.get('labxdb_http_login'), config.get('labxdb_http_password'), config.get('labxdb_http_path'), config.get('labxdb_http_db'))
        trackhub_config, path_trackhub_config = make_config(projects=config.get('projects'), samples=config.get('samples'), replicates=config.get('replicates'), runs=config.get('runs'), levels=config['levels'], path_trackhub_config=config.get('path_trackhub_config'), dbl=dbl, path_root_bams=config['path_root_bams'], bam_folder=config['bam_folder'], bam_names=config['bam_names'], logger=logger)
    else:
        assert 'path_trackhub_config' in config, 'No config file defined. Please specify one using --path_trackhub_config'
        path_trackhub_config = config['path_trackhub_config']
        trackhub_config = json.load(open(path_trackhub_config, 'rt'))

    # Make track hub config files
    if config['make_trackhub']:
        trackhub.create_trackhub(trackhub_config, species=config['species_ucsc'], analyses=config['bam_suffixes'], strands=config['strands'], defaults=config)

    # Generate data
    if config['make_bigwig']:
        # Path to feature for GeneAbacus
        if 'path_features' in config:
            path_features = config['path_features']
        else:
            path_features = config['path_genome']
        # Making track data
        make_bigwig(trackhub_config, config['path_root_bams'], config['bam_folder'], config['bam_names'], config['input_sam'], config['ignore_nh_tag'], config['path_genome'], path_features, path_mapping=config['path_mapping'], strands=config['strands'], profile_type=config['profile_type'], profile_norm=config['profile_norm'], profile_multi=config.get('profile_multi'), profile_untemplated=config.get('profile_untemplated'), profile_no_untemplated=config.get('profile_no_untemplated'), profile_extension_length=config.get('profile_extension_length'), profile_position_fraction=config.get('profile_position_fraction'), read_min_mapping_quality=config.get('read_min_mapping_quality'), read_in_proper_pair=config['read_in_proper_pair'], fragment_min_length=config.get('fragment_min_length'), fragment_max_length=config.get('fragment_max_length'), path_root_output=config['species_ucsc'], export_binary=config['export_binary'], delete_bedgraph=config['delete_bedgraph'], no_count=config['no_count'], update=config['update'], num_processor=config['num_processor'], verbose=config['verbose'], logger=logger)

if __name__ == '__main__':
    sys.exit(main())
