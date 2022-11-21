#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""Merge & normalize count"""

import argparse
import collections
import json
import os
import sys

import numpy as np
import pandas as pd
import zstandard as zstd

import labxdb

import pyfnutils as pfu
import pyfnutils.log

def read_data(path_data, step_name, name_column):
    if step_name == 'counting':
        m = pd.read_csv(path_data+'.csv')
    elif step_name == 'counting_cufflinks':
        m = pd.read_table(path_data+'.fpkm_tracking')
        m.sort([name_column], inplace=True)
        m.index = range(len(m))
    return m

def is_spike_in(prefix, name):
    if name.find(prefix) == -1:
        return True
    else:
        return False

def label2var(label):
    if label is None:
        return None
    else:
        return label.replace(' ', '_').replace('-', '_').replace('%', 'p').replace('/', '_')

def main(argv=None):
    if argv is None:
        argv = sys.argv
    # Started from wrapper?
    prog = os.path.basename(argv[0])
    if argv[1] == 'merge-count':
        job_cmd = argv[:2]
        argv_parser = argv[2:]
        prog += ' merge-count'
    else:
        job_cmd = argv[:1]
        argv_parser = argv[1:]
    # Parse arguments
    parser = argparse.ArgumentParser(prog=prog, description='Merge & Normalize count.')
    parser.add_argument('-c', '--pipeline', dest='path_pipeline', action='store', required=True, help='Path to pipeline')
    parser.add_argument('-s', '--step', dest='step_name', action='store', default='counting', help='Step name')
    parser.add_argument('-l', '--levels', dest='levels', action='store', default='sample,replicate', help='Level to include (comma separated within sample, replicate)')
    parser.add_argument('-a', '--fontools_path_main', dest='fontools_path_main', action='store', default='.', help='Root path to annotation files')
    parser.add_argument('-m', '--columns', dest='columns', action='store', default='count_1,count_2,count_900', help='Column name(s) to merge (comma separated) (default:count_1,count_2,count_900)')
    parser.add_argument('-n', '--name_column', dest='name_column', action='store', default='name', help='Name column (i.e. name or tracking_id)')
    parser.add_argument('-x', '--suffix', dest='suffix', action='store', default='', help='Output filename suffix')
    parser.add_argument('-t', '--strict_column_names', dest='strict_column_names', action='store_true', default=False, help='Use strict column labels (only alphabetic letters, numbers and _)')
    parser.add_argument('-k', '--spike_in_main_prefix', dest='spike_in_main_prefix', action='store', default='', help='Prefix of main gene (other genes are spike-in), i.e. ENSDAR')
    parser.add_argument('-p', '--spike_in_names', dest='spike_in_names', action='store', default='yst,zbf', help='Spike-in and non spike-in normalization filenames, i.e. yst,zbf (comma separated)')
    parser.add_argument('--log', dest='path_log', action='store', help='Path to log')
    parser.add_argument('--path_config', dest='path_config', action='store', help='Path to config')
    parser.add_argument('--http_url', '--labxdb_http_url', dest='labxdb_http_url', action='store', help='Database HTTP URL')
    parser.add_argument('--http_login', '--labxdb_http_login', dest='labxdb_http_login', action='store', help='Database HTTP login')
    parser.add_argument('--http_password', '--labxdb_http_password', dest='labxdb_http_password', action='store', help='Database HTTP password')
    parser.add_argument('--http_path', '--labxdb_http_path', dest='labxdb_http_path', action='store', help='Database HTTP path')
    parser.add_argument('--http_db', '--labxdb_http_db', dest='labxdb_http_db', action='store', help='Database HTTP DB')
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
        if a in ['levels', 'columns', 'spike_in_names']:
            if v is None:
                config[a] = []
            else:
                config[a] = [r.strip() for r in v.split(',')]

    # Logging
    logger = pfu.log.define_root_logger('merge_count', level='info', filename=config.get('path_log'))

    # Init. DBLink
    if 'labxdb_http_path' not in config and 'labxdb_http_db' not in config:
        if 'labxdb_http_path_seq' in config:
            config['labxdb_http_path'] = config['labxdb_http_path_seq']
        else:
            config['labxdb_http_db'] = 'seq'
    dbl = labxdb.DBLink(config.get('labxdb_http_url'), config.get('labxdb_http_login'), config.get('labxdb_http_password'), config.get('labxdb_http_path'), config.get('labxdb_http_db'))

    # Get run config
    pipe_config = json.load(open(config['path_pipeline']))
    run_name = pipe_config['name']
    run_path = pipe_config['path_output']
    # Get step config
    step_config = None
    for s in pipe_config['analysis']:
        if s['step_name'] == config['step_name']:
            step_config = s
    if step_config is None:
        logger.error(f"Could not find «{config['step_name']}» step in pipeline.")
        return 1
    # Get feature names
    feature_names = []
    if 'features' in step_config:
        feature_names = [f['name'] for f in step_config['features']]
    # Get data annotation
    pipe_config_replicate_refs = pipe_config.get('replicate_refs', [])
    pipe_config_replicate_refs.sort()
    pipe_config_run_refs = pipe_config.get('run_refs', [])
    pipe_config_run_refs.sort()
    result = []
    for level, search, refs in [('replicate', '2 replicate_ref', pipe_config_replicate_refs), ('run', '3 run_ref', pipe_config_run_refs)]:
        if len(refs) > 0:
            result += [(level, r) for r in dbl.post('tree', {'search_criterion':[search+' EQUAL '+r for r in refs], 'search_gate':'OR', 'sort_criterion':['1 track_priority ASC', '2 replicate_order ASC', '3 run_order ASC'], 'limit':'ALL'})]

    for feature_name in feature_names:
        # Determine feature_name type based on feature name
        if feature_name.find('gene') != -1:
            feature_name_type = 'gene'
        elif feature_name.find('transcript') != -1:
            feature_name_type = 'transcript'
        else:
            logger.error(f'Could not determine the feature type of {feature_name}.')
            return 1
        logger.info(f'Detected feature: {feature_name_type}')

        # Data
        data = {}

        # Get how to merge data
        merges = []
        merges_labels = set()
        for level, project in result:
            for sample in project['children']:
                merge_sample_refs = []
                for replicate in sample['children']:
                    merge_replicate_refs = []
                    # Open replicate data files
                    if level == 'replicate' and replicate['replicate_ref'] in pipe_config_replicate_refs:
                        merge_replicate_refs.append(replicate['replicate_ref'])
                        if replicate['replicate_ref'] not in data:
                            logger.info(f"Opening {replicate['replicate_ref']} {feature_name}")
                            path_data = os.path.join(run_path, replicate['replicate_ref'], config['step_name'], feature_name)
                            data[replicate['replicate_ref']] = read_data(path_data, config['step_name'], config['name_column'])
                    # Open run data files
                    elif level == 'run':
                        for run in replicate['children']:
                            if run['run_ref'] in pipe_config_run_refs:
                                merge_replicate_refs.append(run['run_ref'])
                                if run['run_ref'] not in data:
                                    logger.info(f"Opening {run['run_ref']} {feature_name}")
                                    path_data = os.path.join(run_path, run['run_ref'], config['step_name'], feature_name)
                                    data[run['run_ref']] = read_data(path_data, config['step_name'], config['name_column'])
                    # Add to merges
                    if 'replicate' in config['levels']:
                        label_short = replicate['label_short']
                        if 'sample' in config['levels'] and label_short == sample['label_short']:
                            label_short += ' R' + str(replicate['replicate_order'])
                        if label_short in merges_labels:
                            logger.warning(f'Duplicate label: {label_short}')
                            label_short += ' dup'
                        if config['strict_column_names']:
                            label_short = label2var(label_short)
                        merges.append({'refs':merge_replicate_refs, 'label_short':label_short, 'level':'replicate'})
                        merges_labels.add(label_short)
                    merge_sample_refs.extend(merge_replicate_refs)
                if 'sample' in config['levels']:
                    label_short = sample['label_short']
                    if label_short in merges_labels:
                        logger.warning(f'Duplicate label: {label_short}')
                        label_short += ' dup'
                    if config['strict_column_names']:
                        label_short = label2var(label_short)
                    merges.append({'refs':merge_sample_refs, 'label_short':label_short, 'level':'sample'})
                    merges_labels.add(label_short)
        logger.info('Merging')
        for merge in merges:
            logger.info(f"{merge['level']: >11}{merge['label_short']: >30}  {','.join(merge['refs'])}")

        # Find annot path from pipeline
        for f in step_config['features']:
            if f['name'] == feature_name:
                annot_fon = f['path_json']
                break

        # Open annotation file
        path_annot = os.path.join(config['fontools_path_main'], 'annots', annot_fon)
        path_annot_fon2 = os.path.join(config['fontools_path_main'], 'annots', annot_fon.replace('fon1', 'fon2'))
        if os.path.exists(path_annot_fon2):
            legacy_fon2 = True
            logger.info(f'Opening {path_annot_fon2}')
            annots = json.load(open(path_annot_fon2))
            Annot = collections.namedtuple('Annot', annots['columns'])
        elif os.path.exists(path_annot):
            legacy_fon2 = False
            logger.info(f'Opening {path_annot}')
            if path_annot.endswith('.zst'):
                annots = json.load(zstd.open(path_annot, 'rt'))
            else:
                annots = json.load(open(path_annot, 'rt'))
        else:
            logger.info(f'{path_annot} not found')
            annots = None

        for main_column in config['columns']:
            logger.info(f'Step {feature_name} {main_column}')
            first_sample = True

            # Init output dataframe
            if first_sample:
                # Get two columns from first run
                main = data[merges[0]['refs'][0]].loc[:,[config['name_column'], 'length']]

                # Renaming main feature to its name
                if feature_name_type == 'gene':
                    main.columns = ['gene', 'gene_length']
                    first_datacol_idx = 2
                elif feature_name_type == 'transcript':
                    main.columns = ['transcript', 'transcript_length']
                    first_datacol_idx = 2

                # Add annotations
                if annots is not None:
                    if feature_name_type == 'gene':
                        # Add gene name
                        if legacy_fon2:
                            dgn = pd.DataFrame([(a.gene_stable_id, a.gene_name) for a in map(Annot._make, annots['annotations'])], columns=['gene', 'gene_name']).drop_duplicates()
                        else:
                            dgn = pd.DataFrame([(a['gene_stable_id'], a['gene_name']) for a in annots['features']], columns=['gene', 'gene_name']).drop_duplicates()
                        main = pd.merge(main, dgn, on='gene', how='left')
                        first_datacol_idx = 3
                    elif feature_name_type == 'transcript':
                        # Add gene ID and name
                        if legacy_fon2:
                            dgn = pd.DataFrame([(a.transcript_stable_id, a.gene_stable_id, a.gene_name) for a in map(Annot._make, annots['annotations'])], columns=['transcript', 'gene', 'gene_name']).drop_duplicates()
                        else:
                            dgn = pd.DataFrame([(a['transcript_stable_id'], a['gene_stable_id'], a['gene_name']) for a in annots['features']], columns=['transcript', 'gene', 'gene_name']).drop_duplicates()
                        main = pd.merge(main, dgn, on='transcript', how='left')
                        first_datacol_idx = 4

                first_sample = False

            # Merge count
            count_labels = []
            count_sums = []
            for merge in merges:
                series = []
                for ref in merge['refs']:
                    m = data[ref]
                    # Check format of series is compatible
                    if np.all(main.loc[:,feature_name_type] == m.loc[:,config['name_column']]):
                        series.append(m.loc[:,main_column])
                    else:
                        logger.warning(f'Format of {sample_ref} incorrect')
                        continue
                count_labels.append(merge['label_short'])
                count_sums.append(np.sum(series, axis=0))

            # Export
            main = pd.concat([main, pd.DataFrame(np.vstack(count_sums).T, columns=count_labels)], axis=1)
            main.to_csv(f"{run_name}_{main_column}_{feature_name}{config['suffix']}.csv", index=False)

            # Normalization
            if config['step_name'] == 'counting':
                if config['spike_in_main_prefix'] != '':
                    spike_in_names = config['spike_in_names']
                    # Normalize by *Spike-in Total*
                    sel_spike = main[feature_name_type].map(lambda x: is_spike_in(config['spike_in_main_prefix'], x))
                    sel_spike[0] = False # Remove total
                    if sel_spike.sum() == 0:
                        logger.error('No spike-in feature found')
                        return 1
                    # Normalize by *Main Total* (Regular RPKM)
                    sel_main = np.invert(sel_spike)
                    sel_main[0] = False # Remove total
                    # Normalize by *(Main + Spike-in) Total*
                    sel_all = sel_main.copy()
                    sel_all.loc[:] = True
                    sel_all[0] = False # Remove total
                    sels = [(spike_in_names[0], '_'+spike_in_names[0], sel_spike), (spike_in_names[1], '_'+spike_in_names[1], sel_main), ('all', '_all', sel_all)]
                else:
                    # Normalize by *Total* (Regular RPKM)
                    sel_all = np.ones(len(main[feature_name_type]), 'bool')
                    sel_all[0] = False # Remove total
                    sels = [('all', '', sel_all)]

                totals_data = []
                for sel_name, sel_suffix, sel in sels:
                    rpkm_labels = []
                    rpkms = []
                    for col in main.columns[first_datacol_idx:]:
                        total = main.loc[sel, col].sum()
                        rpkm = (main.loc[:, col]) * (1000./main.loc[:, feature_name_type+'_length']) * (1000000./total)
                        rpkms.append(rpkm)
                        rpkm_labels.append(col)
                        totals_data.append([col, sel_name, total])

                    # Save dataframe
                    head = main.iloc[:, :first_datacol_idx].copy()
                    rpkm = pd.concat([head, pd.DataFrame(np.vstack(rpkms).T, columns=rpkm_labels)], axis=1)
                    rpkm.to_csv(f"{run_name}_rpkm_{main_column[main_column.find('_')+1:]}_{feature_name}{sel_suffix}{config['suffix']}.csv", index=False)

                # Save totals
                pd.DataFrame(totals_data, columns=['sample', 'name', 'total']).to_csv(f"{run_name}_total_{main_column}_{feature_name}{config['suffix']}.csv", index=False)

if __name__ == '__main__':
    sys.exit(main())
