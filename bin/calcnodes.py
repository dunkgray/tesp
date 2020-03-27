#!/usr/bin/env python3

import click
import math

def calc_nodes_req(granule_count, walltime, workers, hours_per_granule=1.5):
    """ Provides estimation of the number of nodes required to process granule count

    >>> _calc_nodes_req(400, '20:59', 28)
    2
    >>> _calc_nodes_req(800, '20:00', 28)
    3
    """

    hours, _, _ = [int(x) for x in walltime.split(':')]
    return int(math.ceil(float(hours_per_granule * granule_count) / (hours * workers)))

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

@click.group()
def cli():
    pass


@cli.command('calcnodes')
@click.option("--workers", type=click.IntRange(1, 32), default=28,
              help="The number of workers to request per node.")
@click.option("--walltime", default="48:00:00",
              help="Job walltime in `hh:mm:ss` format.")
@click.option("--level1-list", type=click.Path(exists=True, readable=True),
              help="The unfiltered level1 scene list.")
def calcnodes(level1_list, workers, walltime):

    granule_count = file_len(level1_list)
    nodes_req = calc_nodes_req(granule_count, walltime, workers)
    print('Nodes required ', nodes_req)

if __name__ == '__main__':
    calcnodes()
