# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""Interface with the `GeneAbacus <https://git.sr.ht/~vejnar/GeneAbacus>`_ executable program."""

import os
import subprocess

def get_geneabacus_version(exe=None):
    # Defaults
    if exe is None:
        exe = 'geneabacus'
    # Get version
    p = subprocess.run([exe, '--version'], check=True, stdout=subprocess.PIPE, text=True)
    return p.stdout.strip()

def geneabacus(path_bam=None, path_sam=None, sam_command_in=None, path_features=None, format_features=None, fon_name=None, fon_chrom=None, fon_coords=None, fon_strand=None, feature_strand=None, path_features_filter=None, include_missing_in_filter=None, path_mapping=None, path_report=None, read_strand=None, paired=None, ignore_nh_tag=None, read_min_overlap=None, read_min_mapping_quality=None, read_in_proper_pair=None, read_length=None, fragment_min_length=None, fragment_max_length=None, rand_proportion=None, count_path=None, count_multis=None, count_totals=None, count_total_real_read=None, count_in_profile=None, profile_paths=None, profile_type=None, profile_formats=None, profile_multi=None, profile_overhang=None, profile_untemplated=None, profile_no_untemplated=None, profile_extension_length=None, profile_position_fraction=None, profile_norm=None, profile_no_coord_mapping=None, path_sam_out=None, num_worker=None, verbose=None, verbose_level=None, others=None, exe=None, return_std=None, logger=None):
    # Defaults
    if exe is None:
        exe = 'geneabacus'
    if logger is None:
        import logging as logger
    # Check parameters
    assert path_bam is not None or path_sam is not None, 'path_bam/path_sam missing'
    assert not path_features is None, 'path_features missing'
    # ---------
    # GeneAbacus cmd & parameters
    cmd = [exe]
    # Path
    if path_bam is not None:
        if isinstance(path_bam, list):
            cmd.extend(['--path_bam', ','.join(path_bam)])
        else:
            cmd.extend(['--path_bam', path_bam])
    if path_sam is not None:
        if isinstance(path_sam, list):
            cmd.extend(['--path_sam', ','.join(path_sam)])
            first_sam = path_sam[0]
        else:
            cmd.extend(['--path_sam', path_sam])
            first_sam = path_sam.split(',')[0]
        # Input SAM command
        if sam_command_in is not None:
            if isinstance(count_multis, list):
                cmd.extend(['--sam_command_in', ','.join(map(str, sam_command_in))])
            else:
                cmd.extend(['--sam_command_in', sam_command_in])
        else:
            # Add zcat if necessary
            if first_sam.endswith('.gz'):
                cmd.extend(['--sam_command_in', 'zcat'])
            # Add lz4 if necessary
            elif first_sam.endswith('.lz4'):
                cmd.extend(['--sam_command_in', 'lz4cat'])
            # Add zstd if necessary
            elif first_sam.endswith('.zst'):
                cmd.extend(['--sam_command_in', 'zstdcat'])
    cmd.extend(['--path_features', path_features])
    if format_features is not None:
        cmd.extend(['--format_features', format_features])
    if fon_name is not None:
        cmd.extend(['--fon_name', fon_name])
    if fon_chrom is not None:
        cmd.extend(['--fon_chrom', fon_chrom])
    if fon_coords is not None:
        cmd.extend(['--fon_coords', fon_coords])
    if fon_strand is not None:
        cmd.extend(['--fon_strand', fon_strand])
    if feature_strand is not None:
        cmd.extend(['--feature_strand', feature_strand])
    if path_features_filter is not None:
        cmd.extend(['--path_features_filter', path_features_filter])
    if include_missing_in_filter is True:
        cmd.append('--include_missing_in_filter')
    if path_mapping is not None:
        cmd.extend(['--path_mapping', path_mapping])
    if path_report is not None:
        cmd.extend(['--path_report', path_report])
    # Input
    if read_strand is not None:
        cmd.extend(['--read_strand', read_strand])
    if paired is True:
        cmd.append('--paired')
    if ignore_nh_tag is True:
        cmd.append('--ignore_nh_tag')
    # Read selection
    if read_min_overlap is not None:
        cmd.extend(['--read_min_overlap', str(read_min_overlap)])
    if read_min_mapping_quality is not None:
        cmd.extend(['--read_min_mapping_quality', str(read_min_mapping_quality)])
    if read_in_proper_pair is True:
        cmd.append('--read_in_proper_pair')
    if read_length is not None:
        cmd.extend(['--read_length', str(read_length)])
    if fragment_min_length is not None:
        cmd.extend(['--fragment_min_length', str(fragment_min_length)])
    if fragment_max_length is not None:
        cmd.extend(['--fragment_max_length', str(fragment_max_length)])
    if rand_proportion is not None:
        cmd.extend(['--rand_proportion', str(rand_proportion)])
    # Counting
    if count_path is not None:
        cmd.extend(['--count_path', count_path])
    if count_multis is not None:
        if isinstance(count_multis, list):
            cmd.extend(['--count_multis', ','.join(map(str, count_multis))])
        else:
            cmd.extend(['--count_multis', count_multis])
    if count_totals is not None:
        if isinstance(count_totals, list):
            cmd.extend(['--count_totals', ','.join(map(str, count_totals))])
        else:
            cmd.extend(['--count_totals', count_totals])
    if count_total_real_read is True:
        cmd.append('--count_total_real_read')
    if count_in_profile is True:
        cmd.append('--count_in_profile')
    # Profiling
    if profile_paths is not None:
        if isinstance(profile_paths, list):
            cmd.extend(['--profile_paths', ','.join(map(str, profile_paths))])
        else:
            cmd.extend(['--profile_paths', profile_paths])
    if profile_type is not None:
        cmd.extend(['--profile_type', profile_type])
    if profile_formats is not None:
        if isinstance(profile_formats, list):
            cmd.extend(['--profile_formats', ','.join(map(str, profile_formats))])
        else:
            cmd.extend(['--profile_formats', profile_formats])
    if profile_multi is not None:
        cmd.extend(['--profile_multi', profile_multi])
    if profile_overhang is not None:
        cmd.extend(['--profile_overhang', profile_overhang])
    if profile_untemplated is not None:
        cmd.extend(['--profile_untemplated', str(profile_untemplated)])
    if profile_no_untemplated is True:
        cmd.extend(['--profile_no_untemplated'])
    if profile_extension_length is not None:
        cmd.extend(['--profile_extension_length', str(profile_extension_length)])
    if profile_position_fraction is not None:
        cmd.extend(['--profile_position_fraction', str(profile_position_fraction)])
    if profile_norm is True:
        cmd.extend(['--profile_norm'])
    if profile_no_coord_mapping is True:
        cmd.extend(['--profile_no_coord_mapping'])
    # Output
    if path_sam_out is not None:
        cmd.extend(['--path_sam_out', path_sam_out])
    # Number of worker
    if num_worker is not None:
        cmd.append('--num_worker')
        cmd.append(num_worker)
    # Verbose
    if verbose:
        cmd.append('--verbose')
    if verbose_level is not None:
        cmd.extend(['--verbose_level', str(verbose_level)])
    # Add remaining parameters
    if others is not None:
        cmd.extend(others)
    if return_std is None:
        return_std = False
    # Start
    logger.info('Starting GeneAbacus with ' + str(cmd))
    if return_std:
        try:
            p = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return p.stdout, p.stderr
        except Exception as e:
            logger.error('GeneAbacus failed: ' + e.stderr)
            raise
    else:
        subprocess.run(cmd, check=True)
