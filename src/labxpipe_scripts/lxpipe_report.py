#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""Create report with project statistics"""

import argparse
import collections
import datetime
import json
import operator
import os
import sys

import xlsxwriter

import labxdb

def parse_step_level(level, names, report):
    for k, v in level.items():
        if isinstance(v, dict):
            parse_step_level(v, names + [k], report)
        else:
            report['_'.join(names+[k])] = v

def parse_step(report, step_name, all_report):
    step_parsed = collections.OrderedDict()
    parse_step_level(report, [], step_parsed)
    # Sort
    for s in ['input', 'output']:
        for k in list(step_parsed.keys()):
            if s in k:
                step_parsed.move_to_end(k)
    # Add to final report
    for k, v in step_parsed.items():
        all_report[step_name + (k, )] = v

def parsing_reports(config, time_fmt='delta', spreadsheet=True, completion_time_format='%Y-%m-%d %H:%M:%S', http_url=None, http_login=None, http_password=None, http_path=None, http_db=None):
    # LabxDB parameters
    if http_path is None and http_db is None:
        http_db = 'seq'
    #  Init. DBLink
    dbl = labxdb.DBLink(http_url, http_login, http_password, http_path, http_db)

    reports = []
    for seq_level, seq_ref in [('run', r) for r in sorted(config.get('run_refs', []))] + [('replicate', r) for r in sorted(config.get('replicate_refs', []))]:
        path_root = os.path.join(config['path_output'], seq_ref)
        all_report = {}
        if seq_level == 'run':
            all_report[('run', '')] = seq_ref
        elif seq_level == 'replicate':
            all_report[('replicate', '')] = seq_ref
        # Get annots for run
        if 'db' in config['ref_info_source']:
            if seq_level == 'run':
                # Query: Run
                run = dbl.get('run/get-ref/'+seq_ref)[0][0]
                # Get replicate ref.
                replicate_ref = run['replicate_ref']
            elif seq_level == 'replicate':
                replicate_ref = seq_ref
                # Query: All run(s)
                runs = dbl.post('run', {'search_criterion':['3 replicate_ref EQUAL '+replicate_ref], 'sort_criterion':['3 run_order ASC'], 'limit':'ALL'})
                # Get first run
                run = runs[0]
            # Query: Replicate
            replicate = dbl.get('replicate/get-ref/'+replicate_ref)[0][0]
            # Copy-paste info to config
            all_report[('replicate', '')] = replicate['replicate_ref']
            all_report[('sample', '')] = replicate['sample_ref']
            all_report[('label_short', '')] = replicate['label_short']
            all_report[('failed', '')] = run['failed']
        if 'json' in config['ref_info_source']:
            all_report[('replicate', '')] = None
            all_report[('sample', '')] = None
            all_report[('label_short', '')] = None
            all_report[('failed', '')] = None
            if seq_ref in config['ref_infos']:
                if 'replicate' in config['ref_infos'][seq_ref]:
                    all_report[('replicate', '')] = config['ref_infos'][seq_ref]['replicate_ref']
                if 'sample' in config['ref_infos'][seq_ref]:
                    all_report[('sample', '')] = config['ref_infos'][seq_ref]['sample_ref']
                if 'label_short' in config['ref_infos'][seq_ref]:
                    all_report[('label_short', '')] = config['ref_infos'][seq_ref]['label_short']
                if 'failed' in config['ref_infos'][seq_ref]:
                    all_report[('failed', '')] = config['ref_infos'][seq_ref]['failed']
        # Get counts
        for step in config['analysis']:
            # Report
            path_report = os.path.join(path_root, step['step_name'], step['step_name']+'_report.json')
            # Parse report
            if os.path.exists(path_report):
                parse_step(json.load(open(path_report)), (step['step_name'], ), all_report)
                # Percent for spreadsheet
                if spreadsheet:
                    all_report[(step['step_name'], '%')] = None
            elif step['step_name'] == 'counting':
                for feat in step['features']:
                    path_report = os.path.join(path_root, step['step_name'], feat['name']+'_report.json')
                    if os.path.exists(path_report):
                        parse_step(json.load(open(path_report)), (step['step_name'], feat['name']), all_report)
                        if spreadsheet:
                            all_report[(step['step_name'], feat['name'], '%')] = None
        # Get computing time
        path_compl = os.path.join(path_root, 'log', config['name']+'_compl.json')
        if os.path.exists(path_compl):
            compl_steps = json.load(open(path_compl))
            total_time = datetime.timedelta(0)
            for step in compl_steps:
                if step['start'] and step['end']:
                    delta = datetime.datetime.strptime(step['end'], completion_time_format) - datetime.datetime.strptime(step['start'], completion_time_format)
                    total_time += delta
                    if time_fmt == 'delta':
                        all_report[(step['step_name'], 'time')] = delta
                    elif time_fmt == 'days':
                        all_report[(step['step_name'], 'time')] = delta.total_seconds() /60./60./24.
                    else:
                        all_report[(step['step_name'], 'time')] = str(delta)
            if time_fmt == 'delta':
                all_report[('total', 'time')] = total_time
            elif time_fmt == 'days':
                all_report[('total', 'time')] = total_time.total_seconds() /60./60./24.
            else:
                all_report[('total', 'time')] = str(total_time)
        reports.append(all_report)

    return reports

def export_csv(reports, path_output='report'):
    with open(path_output+'.csv', 'wt') as fout:
        # Header
        longest_report = max(enumerate([len(r) for r in reports]),key=lambda x: x[1])[0]
        headers = list(reports[longest_report].keys())
        fout.write(','.join(['_'.join([c for c in h if c != '']) for h in headers]) + '\n')
        # Content
        for report in reports:
            fout.write(','.join([str(report.get(col)) for col in headers]) + '\n')

def export_xls(reports, path_output='report'):
    workbook = xlsxwriter.Workbook(path_output+'.xlsx')
    worksheet = workbook.add_worksheet()

    # Formats
    format_default = workbook.add_format()
    format_default.set_font_size(9)
    format_default.set_font_name('Arial')
    format_default.set_border(1)
    format_default.set_border_color('#B5B5B5')
    format_header = workbook.add_format()
    format_header.set_font_size(9)
    format_header.set_font_name('Arial')
    format_header.set_border(1)
    format_header.set_border_color('#B5B5B5')
    format_header.set_text_wrap()
    format_header.set_align('vcenter')
    format_count = workbook.add_format()
    format_count.set_font_size(9)
    format_count.set_font_name('Arial')
    format_count.set_bg_color('#E1E1E1')
    format_count.set_border(1)
    format_count.set_border_color('#B5B5B5')
    format_count.set_num_format('#,##0')
    format_percent = workbook.add_format()
    format_percent.set_font_size(9)
    format_percent.set_font_name('Arial')
    format_percent.set_border(1)
    format_percent.set_border_color('#B5B5B5')
    format_percent.set_num_format('0.0')
    format_time = workbook.add_format()
    format_time.set_font_size(9)
    format_time.set_font_name('Arial')
    format_time.set_border(1)
    format_time.set_border_color('#B5B5B5')
    format_time.set_num_format('[hh]:mm')

    worksheet.set_row(0, 60, format_header)

    global_row = 0

    # Prepare headers
    longest_report = max(enumerate([len(r) for r in reports]),key=lambda x: x[1])[0]
    headers = list(reports[longest_report].keys())

    # Write headers
    for ih in range(len(headers)):
        if headers[ih][0] in ('run', 'replicate', 'sample', 'failed'):
            worksheet.set_column(ih, ih, 8, format_default)
        elif headers[ih][0] in ('label_short'):
            worksheet.set_column(ih, ih, 20, format_default)
        elif '%' in headers[ih]:
            worksheet.set_column(ih, ih, 3.5, format_percent)
        elif 'time' in headers[ih]:
            worksheet.set_column(ih, ih, 5, format_time)
        elif headers[ih][1] != '':
            worksheet.set_column(ih, ih, 8.5, format_count)
        else:
            worksheet.set_column(ih, ih, 3, format_default)
        worksheet.write(global_row, ih, '_'.join([h for h in headers[ih] if h != '']))
    global_row += 1

    # Write counts
    for report in reports:
        for icol in range(len(headers)):
            step = headers[icol][0]
            if '%' in headers[icol]:
                r1 = xlsxwriter.utility.xl_rowcol_to_cell(global_row, icol-1)
                r2 = xlsxwriter.utility.xl_rowcol_to_cell(global_row, icol-2)
                worksheet.write_formula(global_row, icol, f'={r1}/{r2}*100', value='')
            else:
                if headers[icol] in report:
                    worksheet.write(global_row, icol, report[headers[icol]])
        global_row += 1

    # Write total
    for icol in range(len(headers)):
        if headers[icol][0] in ('run', 'replicate', 'sample', 'label_short', 'failed'):
            continue
        elif '%' in headers[icol]:
            r1 = xlsxwriter.utility.xl_rowcol_to_cell(1, icol)
            r2 = xlsxwriter.utility.xl_rowcol_to_cell(global_row, icol)
            worksheet.write_formula(global_row+1, icol, f'=SUMIF({r1}:{r2},">0")/COUNTIF({r1}:{r2},">0")', value='')
        else:
            r1 = xlsxwriter.utility.xl_rowcol_to_cell(1, icol)
            r2 = xlsxwriter.utility.xl_rowcol_to_cell(global_row, icol)
            worksheet.write_formula(global_row+1, icol, f'=SUM({r1}:{r2})', value='')
    worksheet.write(global_row+1, 0, 'TOTAL')

    workbook.close()

def main(argv=None):
    if argv is None:
        argv = sys.argv
    # Started from wrapper?
    prog = os.path.basename(argv[0])
    if argv[1] == 'report':
        job_cmd = argv[:2]
        argv_parser = argv[2:]
        prog += ' report'
    else:
        job_cmd = argv[:1]
        argv_parser = argv[1:]
    # Parse arguments
    parser = argparse.ArgumentParser(prog=prog, description='Generate pipeline report.')
    parser.add_argument('-c', '--pipeline', dest='path_pipeline', action='store', required=True, help='Path to pipeline')
    parser.add_argument('-f', '--report_format', dest='report_format', action='store', default='xls', help='Report format: xls or csv')
    parser.add_argument('--path_config', dest='path_config', action='store', help='Path to config')
    parser.add_argument('--http_url', '--labxdb_http_url', dest='labxdb_http_url', action='store', help='Database HTTP URL')
    parser.add_argument('--http_login', '--labxdb_http_login', dest='labxdb_http_login', action='store', help='Database HTTP login')
    parser.add_argument('--http_password', '--labxdb_http_password', dest='labxdb_http_password', action='store', help='Database HTTP password')
    parser.add_argument('--http_path', '--labxdb_http_path', dest='labxdb_http_path', action='store', help='Database HTTP path')
    parser.add_argument('--http_db', '--labxdb_http_db', dest='labxdb_http_db', action='store', help='Database HTTP DB')
    args = parser.parse_args(argv_parser)

    # Load config: Global (JSON single file or all files in path_config)
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

    # LabxDB parameters
    if 'labxdb_http_path' not in config and 'labxdb_http_db' not in config:
        if 'labxdb_http_path_seq' in config:
            config['labxdb_http_path'] = config['labxdb_http_path_seq']
        else:
            config['labxdb_http_db'] = 'seq'

    # Load config: Project
    if not os.path.exists(config['path_pipeline']):
        print('ERROR: Pipeline file not found')
        return 1
    else:
        config = {**config, **json.load(open(config['path_pipeline']))}

    # Parse all reports
    if config['report_format'] == 'csv':
        spreadsheet = False
    else:
        spreadsheet = True
    reports = parsing_reports(config, 'days', spreadsheet, http_url=config.get('labxdb_http_url'), http_login=config.get('labxdb_http_login'), http_password=config.get('labxdb_http_password'), http_path=config.get('labxdb_http_path'), http_db=config.get('labxdb_http_db'))

    # Output reports
    if config['report_format'] == 'csv':
        export_csv(reports, config['path_pipeline'][:len('.json')*-1])
    elif config['report_format'] == 'xls':
        export_xls(reports, config['path_pipeline'][:len('.json')*-1])

if __name__ == '__main__':
    sys.exit(main())
