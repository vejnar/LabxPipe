# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""UCSC track hub."""

import os

class Node(object):
    fields = []

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def render(self, indent=''):
        return '\n'.join([f'{indent}{k} {getattr(self, k)}' for k in self.fields if k in dir(self) and getattr(self, k) is not None])

class Hub(Node):
    fields = ['hub',
              'shortLabel',
              'longLabel',
              'genomesFile',
              'email']
    
    def __init__(self, name, genome_names, **kwargs):
        self.name = name
        kwargs['genomesFile'] = 'genomes.txt'
        Node.__init__(self, **kwargs)
        self.genomes = [Genome(genome = g, trackDb = f'{g}/trackDb.txt') for g in genome_names]
        self.tracks = {g: [] for g in genome_names}

    def write(self, make_species_folder=True):
        # Hub
        with open('hub.txt', 'wt') as f:
            f.write(self.render() + '\n')
        # Genome
        with open('genomes.txt', 'wt') as f:
            for g in self.genomes:
                f.write(g.render() + '\n')
        # Track
        for g, tracks in self.tracks.items():
            fpath = 'trackDb.txt'
            if make_species_folder:
                species_folder = g
                if not os.path.exists(species_folder):
                    os.mkdir(species_folder)
            else:
                species_folder = '.'
            with open(os.path.join(species_folder, fpath), 'wt') as f:
                for t in tracks:
                    f.write(t.render() + '\n\n')

class Genome(Node):
    fields = ['genome',
              'trackDb']
    
class Track(Node):
    fields = ['track',
              'container',
              'parent',
              'type',
              'bigDataUrl',
              'shortLabel',
              'longLabel',
              'configurable',
              'visibility',
              'maxHeightPixels',
              'itemRgb',
              'aggregate',
              'showSubtrackColorOnUi',
              'autoScale',
              'windowingFunction',
              'priority',
              'alwaysZero',
              'color',
              'subGroup1',
              'subGroups']

def get_parent_track(track = '',
                     container = 'multiWig',
                     shortLabel = '',
                     longLabel = '',
                     type = 'bigWig 0 90000',
                     configurable = 'on',
                     visibility = 'hide',
                     maxHeightPixels = '150:50:20',
                     aggregate = 'none',
                     showSubtrackColorOnUi = 'on',
                     autoScale = 'on',
                     windowingFunction = 'mean',
                     priority = '1',
                     subGroup1 = 'level Level sample=Sample replicate=Replicate'):
    return Track(track = track,
                 container = container,
                 shortLabel = shortLabel,
                 longLabel = longLabel,
                 type = type,
                 configurable = configurable,
                 visibility = visibility,
                 maxHeightPixels = maxHeightPixels,
                 aggregate = aggregate,
                 showSubtrackColorOnUi = showSubtrackColorOnUi,
                 autoScale = autoScale,
                 windowingFunction = windowingFunction,
                 priority = priority,
                 subGroup1 = subGroup1)

def get_track(track = '',
              type = 'bigWig',
              bigDataUrl = '',
              shortLabel = '',
              longLabel = ''):
    return Track(track = track,
                 type = type,
                 bigDataUrl = bigDataUrl,
                 shortLabel = shortLabel,
                 longLabel = longLabel)

def create_trackhub(config,
                    species = 'danRer11',
                    analyses = ['', 'unique'],
                    strands = ['combined', 'minus', 'plus'],
                    defaults = {},
                    group_priority = 100):
    # Parameters
    hub_name = config['project']['project_name']
    # Init hub
    hub = Hub(hub_name,
              [species],
              hub = defaults.get('prefix_hub', '') + hub_name,
              shortLabel = defaults.get('prefix_short_label', '') + config['project']['label_short'],
              longLabel = defaults.get('prefix_long_label', '') + config['project']['label_long'],
              email = defaults.get('prefix_email', '') + config['project'].get('email', ''))
    # Generate tracks
    for aname in analyses:
        if aname == '' or aname is None:
            aname_prefix_lsep = 0
            aname_prefix_short = ''
            aname_id = ''
            aname_name = ''
        else:
            aname_prefix_lsep = 1
            aname_prefix_short = aname[0]
            aname_id = '_' * aname_prefix_lsep + aname
            aname_name = ' ' * aname_prefix_lsep + aname

        # Generate parent tracks
        if 'combined' in strands:
            pt = get_parent_track()
            pt.track = hub_name + '_multiwig' + aname_id.title()
            pt.shortLabel = (f'[{aname_prefix_short}]') * aname_prefix_lsep + config['project']['label_short']
            pt.longLabel = config['project']['label_long'] + aname_name.title()
            pt.visibility = config['project'].get('combined_visibility')
            pt.priority = str(group_priority)
            group_priority += 1
            hub.tracks[species].append(pt)
        if 'minus' in strands or 'plus' in strands:
            pt = get_parent_track()
            pt.track = hub_name + '_multiwig_Strand_Separated' + aname_id.title()
            pt.shortLabel = f'[SS{aname_prefix_short}]' + config['project']['label_short']
            pt.longLabel = config['project']['label_long'] + ' Strand Separated' + aname_name.title()
            pt.visibility = config['project'].get('strand_visibility')
            pt.priority = str(group_priority)
            group_priority += 1
            hub.tracks[species].append(pt)

        # Generate data tracks
        for track in config['tracks']:
            if 'combined' in strands:
                t = get_track()
                t.track = f"{track['name']}{aname_id}_bigWig"
                t.bigDataUrl = f"{track['data']}{aname_id}.bw"
                t.shortLabel = (f'[{aname_prefix_short}]') * aname_prefix_lsep + track['label_short']
                t.longLabel = track['label_long'] + aname_name.title()
                t.parent = hub_name + '_multiwig' + aname_id.title()
                t.type = 'bigWig'
                t.priority = track['track_priority']
                if 'combined_visibility' in config['project'] and config['project']['combined_visibility'] != 'hide':
                    t.visibility = track.get('track_visibility')
                t.color = defaults['colors'].get(track['track_color'], track['track_color'])
                t.alwaysZero = 'on'
                t.subGroups = 'level=' + track['level']
                hub.tracks[species].append(t)
            strand_labels = []
            if 'minus' in strands:
                strand_labels.append(('minus', '[-%s]', '(- strand)'))
            if 'plus' in strands:
                strand_labels.append(('plus', '[+%s]', '(+ strand)'))
            for strand_name, strand_label_short, strand_label_long in strand_labels:
                t = get_track()
                t.track = f"{track['name']}_{strand_name}{aname_id}_bigWig"
                t.bigDataUrl = f"{track['data']}_{strand_name}{aname_id}.bw"
                t.shortLabel = strand_label_short%aname_prefix_short + track['label_short']
                t.longLabel = track['label_long'] + ' ' + strand_label_long + aname_name.title()
                t.parent = hub_name + '_multiwig_Strand_Separated' + aname_id.title()
                t.type = 'bigWig'
                t.priority = track['track_priority']
                if 'strand_visibility' in config['project'] and config['project']['strand_visibility'] != 'hide':
                    t.visibility = track.get('track_visibility')
                t.color = defaults['colors'].get(track['track_color'], track['track_color'])
                t.alwaysZero = 'on'
                t.subGroups = 'level=' + track['level']
                hub.tracks[species].append(t)

    # Write hub
    hub.write()
