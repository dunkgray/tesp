#!/usr/bin/env python3

import io
import os
import logging
import math
import tempfile
import zipfile
import sys
from xml.etree import ElementTree
from datetime import datetime
from os.path import join, basename
from subprocess import Popen, PIPE, check_output, CalledProcessError
from pathlib import Path

import click
from click_datetime import Datetime
from dateutil.parser import parse as date_parser

from wagl.acquisition import acquisitions
from tesp.workflow import Package
from eodatasets.prepare.s2_prepare_cophub_zip import _process_datasets


DEFAULT_S2_AOI = '/g/data/v10/eoancillarydata/S2_extent/S2_aoi.csv'
DEFAULT_S2_L1C = '/g/data/fj7/Copernicus/Sentinel-2/MSI/L1C'
DEFAULT_WORKDIR = '/g/data/if87/datacube/002/S2_MSI_ARD/workdir'
DEFAULT_LOGDIR = '/g/data/if87/datacube/002/S2_MSI_ARD/log_dir'
DEFAULT_PKGDIR = '/g/data/if87/datacube/002/S2_MSI_ARD/packaged'


def get_archive_metadata(pathname: Path):
    """ Code to extract granule names from L1C Metadata, and processing baseline

    Logic has been ported from wagl.acquisition.__init__
    to avoid the overhead of caching the per measurement metadata
    methods in wagl.acquisition should be refactored to export
    this functionality

    returns a list of granule names
    """
    archive = zipfile.ZipFile(str(pathname))
    xmlfiles = [s for s in archive.namelist() if "MTD_MSIL1C.xml" in s]
    if not xmlfiles:
        pattern = basename(str(pathname).replace('PRD_MSIL1C', 'MTD_SAFL1C'))
        pattern = pattern.replace('.zip', '.xml')
        xmlfiles = [s for s in archive.namelist() if pattern in s]

    mtd_xml = archive.read(xmlfiles[0])
    xml_root = ElementTree.XML(mtd_xml)

    search_term = './*/Product_Info/Product_Organisation/Granule_List/Granules'
    grn_elements = xml_root.findall(search_term)

    # handling multi vs single granules + variants of each type
    if not grn_elements:
        grn_elements = xml_root.findall(search_term[:-1])

    if grn_elements[0].findtext('IMAGE_ID'):
        search_term = 'IMAGE_ID'
    else:
        search_term = 'IMAGE_FILE'

    # required to identify granule metadata in a multigranule archive
    # in the earlier l1c products
    processing_baseline = xml_root.findall('./*/Product_Info/PROCESSING_BASELINE')[0].text

    results = {}
    for granule in grn_elements:
        #print(ElementTree.tostring(granule))
        gran_id = granule.get('granuleIdentifier')
        if not pathname.suffix == '.zip':
            gran_path = str(pathname.parent.joinpath('GRANULE', gran_id, gran_id[:-7].replace('MSI', 'MTD') + '.xml'))
            root = ElementTree.parse(gran_path).getroot()
        else:
            xmlzipfiles = [s for s in archive.namelist() if 'MTD_TL.xml' in s]
            if not xmlzipfiles:
                pattern = gran_id.replace('MSI', 'MTD')
                pattern = pattern.replace('_N' + processing_baseline, '.xml')
                xmlzipfiles = [s for s in archive.namelist() if pattern in s]
            mtd_xml = archive.read(xmlzipfiles[0])
            root = ElementTree.XML(mtd_xml)
        sensing_time = root.findall('./*/SENSING_TIME')[0].text
        results[gran_id] = date_parser(sensing_time)

    return results


def filter_granules(out_stream,
                    raw_zips,
                    good_tile_ids,
                    pkgdir):
    count = 0
    
    for level1_dataset in raw_zips:
        count = filter_granule(out_stream,
                               level1_dataset,
                               good_tile_ids,
                               pkgdir,
                               count)
    return out_stream, count

def filter_granule(out_stream,
                   level1_dataset,
                   good_tile_ids,
                   pkgdir,
                   count):
    try:
        container = acquisitions(str(level1_dataset))
    except Exception as e:
        logging.warning('encountered unexpected error for %s: %s', str(level1_dataset), e)
        logging.exception(e)
        return count

    granule_md = get_archive_metadata(level1_dataset)

    for granule, sensing_date in granule_md.items():
        tile_id = granule.split('_')[-2]
        if tile_id not in good_tile_ids:
            logging.info('granule %s with MGRS tile ID %s outside AOI', granule, tile_id)
            return count

        ymd = sensing_date.strftime('%Y-%m-%d')
        package = Package(
            level1=str(level1_dataset),
            workdir='',
            granule=granule,
            pkgdir=join(pkgdir, ymd)
        )
        if package.output().exists():
            logging.debug('granule %s already processed', granule)
            return count

        logging.info('level1 dataset %s needs to be processed', level1_dataset)
        print(level1_dataset, file=out_stream)
        count += len(granule_md.keys())  # To handle multigranule files
        break
    return count

@click.group()
def cli():
    pass


@cli.command('filter')
@click.option("--level1-list", type=click.Path(exists=True, readable=True),
              help="The unfiltered level1 scene list.")
@click.option('--s2-aoi', default=DEFAULT_S2_AOI, type=str,
              help="List of MGRS tiles of interest.")
@click.option('--pkgdir', default=DEFAULT_PKGDIR, type=click.Path(file_okay=False),
              help="The base output packaged directory.")
@click.option("--workdir", default=DEFAULT_WORKDIR, type=click.Path(file_okay=False, writable=True),
              help="The base output working directory.")
@click.option("--logdir", default=DEFAULT_LOGDIR, type=click.Path(file_okay=False, writable=True),
              help="The base logging and scripts output directory.")
@click.option('--file-prefix', default='filtered', type=str,
              help="The prefix of the output file.")
def filter(level1_list, s2_aoi, workdir, logdir, pkgdir, file_prefix):
    
    click.echo(' '.join(sys.argv))

    logging.basicConfig(format='%(asctime)s %(levelname)s (%(pathname)s:%(lineno)s) %(message)s', level=logging.INFO)
    
    with open(level1_list, 'r') as src:
        raw_zips = [Path(p.strip()) for p in src.readlines()]
    #print (paths)

    out_stream = tempfile.NamedTemporaryFile(mode="w+",
                                             prefix=file_prefix,
                                             suffix='.txt',
                                             delete=False,
                                             dir=workdir,)
    # Read area of interest list
    with open(s2_aoi) as csv:
        good_tile_ids = {'T' + tile.strip() for tile in csv}
    filter_granules(out_stream,
                    raw_zips,
                    good_tile_ids,
                    pkgdir)
    out_stream.flush()
    #out_stream.close()
    print(out_stream.name)
    logging.info('finished')
if __name__ == '__main__':
    filter()
