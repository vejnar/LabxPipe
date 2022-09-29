# <img src="https://raw.githubusercontent.com/vejnar/LabxPipe/main/img/logo.svg" alt="LabxPipe" width="45%" />

[![MPLv2](https://img.shields.io/aur/license/LabxPipe?color=1793d1&style=for-the-badge)](https://mozilla.org/MPL/2.0/)

* Integrated with [LabxDB](https://labxdb.vejnar.org): all required annotations (labels, strand, paired etc) are retrieved from LabxDB. This is optional.
* Based on existing robust technologies. No new language.
    * LabxPipe pipelines are defined in JSON text files.
    * LabxPipe is written in Python. Using norms, such as input and output filenames, insures compatibility between tasks.
* Simple and complex pipelines.
    * By default, pipelines are linear (one step after the other).
    * Branching is easily achieved be defining a previous step (using `step_input` parameter) allowing users to create any dependency between tasks.
* Parallelized using robust asynchronous threads from the Python standard library.

## Examples

See JSON files in `config/pipelines` of this repository.

| Pipeline JSON file            |                                                                                                                           |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `mrna_seq.json`               | mRNA-seq                                                                                                                  |
| `mrna_seq_no_db.json`         | mRNA-seq. No [LabxDB](https://labxdb.vejnar.org)                                                                          |
| `mrna_seq_with_plotting.json` | mRNA-seq. Plotting non-mapped reads. Demonstrate `step_input`                                                             |
| `mrna_seq_cufflinks.json`     | mRNA-seq. Replaces GeneAbacus by Cufflinks                                                                                |
| `chip_seq.json`               | ChIP-seq using [Bowtie2](https://github.com/BenLangmead/bowtie2) and [Samtools](http://www.htslib.org) to uniquify reads. |

Following demonstrates how to apply `mrna_seq.json` pipeline. It requires:
* [LabxDB](https://labxdb.vejnar.org)
* FASTQ files for sample named `AGR000850` and `AGR000912`
    ```
    /plus/data/seq/by_run/AGR000850
    ├── 23_009_R1.fastq.zst
    └── 23_009_R2.fastq.zst
    /plus/data/seq/by_run/AGR000912
    ├── 65_009_R1.fastq.zst
    └── 65_009_R2.fastq.zst
    ```

Note: `mrna_seq_no_db.json` demonstrates how to use LabxPipe *without* LabxDB: it only requires FASTQ files (in `path_seq_run` directory, see above).

Requirements:
* [LabxDB](https://labxdb.vejnar.org). Alternatively, `mrna_seq_no_db.json` doesn't require LabxDB.
* [ReadKnead](https://sr.ht/~vejnar/ReadKnead) to trim reads.
* [STAR](https://github.com/alexdobin/STAR) and genome index in directory defined `path_star_index`.
* [GeneAbacus](https://sr.ht/~vejnar/GeneAbacus) to count reads and generate genomic profile for tracks.

1. Start pipeline:
    ```bash
    lxpipe run --pipeline mrna_seq.json \
               --worker 2 \
               --processor 16
    ```
    Output is written in `path_output` directory.
2. Create report:
    ```bash
    lxpipe report --pipeline mrna_seq.json
    ```
    Report file `mrna_seq.xlsx` should be created in same directory as `mrna_seq.json`.
3. Merge gene/mRNA counts generated by [GeneAbacus](https://sr.ht/~vejnar/GeneAbacus) in `counting` directory:
    ```bash
    lxpipe merge-count --pipeline mrna_seq.json \
                       --step counting
    ```
4. Trackhub. Requirements:
    * [ChromosomeMappings](https://github.com/dpryan79/ChromosomeMappings) file (to map chromosome names from Ensembl/NCBI to UCSC)
    * Tabulated file (with chromosome name and length)

    Execute in a separate directory:
    ```bash
    lxpipe trackhub --runs AGR000850,AGR000912 \
                    --species_ucsc danRer11 \
                    --path_genome /plus/scratch/sai/annots/danrer_genome_all_ensembl_grcz11_ucsc_chroms_chrom_length.tab \
                    --path_mapping /plus/scratch/sai/annots/ChromosomeMappings/GRCz11_ensembl2UCSC.txt \
                    --input_sam \
                    --bam_names accepted_hits.sam.zst \
                    --make_config \
                    --make_trackhub \
                    --make_bigwig \
                    --processor 16
    ```
    Directory is ready to be shared by a web server for display in the [UCSC genome browser](https://genome.ucsc.edu/cgi-bin/hgHubConnect).

## Configuration

Parameters can be defined [globally](https://labxdb.vejnar.org/doc/install/python/#configuration). See in `config` directory of this repository for examples.

## Writing pipelines

Parameters are defined first globally (see above), then per pipeline, then per replicate/run, and then per step/function. The latest definition takes precedence: `path_seq_run` defined in `/etc/hts/labxpipe.json` is used by default, but if `path_seq_run` is defined in the pipeline file, it will be used instead.

Main parameters

| Parameter          | Type          |
| ------------------ | ------------- |
| name               | string        |
| path_output        | string        |
| path_seq_run       | string        |
| path_annots        | string        |
| path_bowtie2_index | string        |
| path_star_index    | string        |
| fastq_exts         | []strings     |
| adaptors           | {}            |
| logging_level      | string        |
| run_refs           | []strings     |
| replicate_refs     | []strings     |
| ref_info_source    | []strings     |
| ref_infos          | {}            |
| analysis           | [{}, {}, ...] |

Parameters for all functions

| Parameter     | Type    |
| ------------- | ------- |
| step_name     | string  |
| step_function | string  |
| step_desc     | string  |
| force         | boolean |

Function-specific parameters

| Function           | Synonym          | Parameter             | Type          |
| ------------------ | ---------------- | --------------------- | ------------- |
| readknead          | preparing        | options               | []strings     |
|                    |                  | ops_r1                | [{}, {}, ...] |
|                    |                  | ops_r2                | [{}, {}, ...] |
|                    |                  | plot_fastq_in         | boolean       |
|                    |                  | plot_fastq            | boolean       |
|                    |                  | fastq_out             | boolean       |
|                    |                  | zip_fastq_out         | string        |
| bowtie2            | genomic_aligning | options               | []strings     |
|                    |                  | index                 | string        |
|                    |                  | output                | string        |
|                    |                  | output_unfiltered     | string        |
|                    |                  | compress_sam          | boolean       |
|                    |                  | compress_sam_cmd      | string        |
|                    |                  | create_bam            | boolean       |
|                    |                  | index_bam             | boolean       |
| star               | aligning         | options               | []strings     |
|                    |                  | index                 | string        |
|                    |                  | output_type           | []strings     |
|                    |                  | compress_sam          | boolean       |
|                    |                  | compress_sam_cmd      | string        |
|                    |                  | compress_unmapped     | boolean       |
|                    |                  | compress_unmapped_cmd | string        |
| cufflinks          |                  | options               | []strings     |
|                    |                  | inputs                | [{}, {}, ...] |
|                    |                  | features              | [{}, {}, ...] |
| geneabacus         | counting         | options               | []strings     |
|                    |                  | inputs                | [{}, {}, ...] |
|                    |                  | features              | [{}, {}, ...] |
| uniquify           |                  | options               | []strings     |
|                    |                  | sort_by_name_bam      | boolean       |
|                    |                  | index_bam             | boolean       |
| cleaning           |                  | steps                 | [{}, {}, ...] |

Sample-specific parameters. Automatically populated if using LabxDB or sourced from `ref_infos`. These parameters can be changed manually in any function (for example setting `paired` to `False` will ignore second reads in that step).

| Parameter      | Type    |
| -------------- | ------- |
| label_short    | string  |
| paired         | boolean |
| directional    | boolean |
| r1_strand      | string  |
| quality_scores | string  |

## License

*LabxPipe* is distributed under the Mozilla Public License Version 2.0 (see /LICENSE).

Copyright (C) 2013-2022 Charles E. Vejnar
