#! /usr/bin/env python3


# eurostat-fetcher -- Fetch series from Eurostat database
# By: Christophe Benz <christophe.benz@cepremap.org>
#
# Copyright (C) 2017 Cepremap
# https://git.nomics.world/dbnomics-fetchers/eurostat-fetcher
#
# eurostat-fetcher is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# eurostat-fetcher is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


"""Convert Eurostat provider, categories, datasets and time series to DBnomics JSON and TSV files."""


import argparse
import binascii
import hashlib
import logging
import os
import re
import shutil
import sqlite3
import struct
import subprocess
import sys
import time
import zlib
from collections import OrderedDict
from io import StringIO
from pathlib import Path

import humanize
from lxml import etree
from toolz import get_in, valmap

import ujson as json
from dbnomics_data_model import observations
from dbnomics_data_model.series import SERIES_JSONL_FILE_NAME
from dbnomics_data_model.storages import indexes

provider_code = 'Eurostat'
provider_json = {
    "code": provider_code,
    "name": provider_code,
    "region": "European Union",
    "terms_of_use": "http://ec.europa.eu/eurostat/about/policies/copyright",
    "website": "http://ec.europa.eu/eurostat/home",
}

args = None  # Will be defined by main().
datasets_dir_name = "data"
log = logging.getLogger(__name__)
namespace_url_by_name = {"xml": "http://www.w3.org/XML/1998/namespace"}
timings = None


def convert_sdmx_element(element, dataset_json, dataset_context, dsd_infos, series_jsonl_file):
    global timings

    # Due to event=end, given to iterparse, we receive <Obs> then <Series> elements, in this order.

    if element.tag.endswith("Series"):

        # Ignore some specific XML element attributes corresponding to series SDMX attributes,
        # because series SDMX attributes do not exist in DBnomics.
        series_element_attributes = OrderedDict([
            (attribute_key, attribute_value)
            for attribute_key, attribute_value in element.attrib.items()
            if attribute_key not in {"TIME_FORMAT"}  # Redundant with FREQ.
        ])

        dimensions_codes_order = list(series_element_attributes.keys())
        if dataset_json["dimensions_codes_order"] is None:
            dataset_json["dimensions_codes_order"] = dimensions_codes_order
        else:
            # dimensions_codes_order must not change between series.
            assert dataset_json["dimensions_codes_order"] == dimensions_codes_order, \
                (dataset_json["dimensions_codes_order"], dimensions_codes_order)

        # Fill series dimensions labels in dataset.json.

        t0 = time.time()

        for dimension_code, dimension_value_code in series_element_attributes.items():
            if dimension_code not in dataset_json["dimensions_labels"]:
                dimension_label = dsd_infos["concepts"].get(dimension_code)
                if dimension_label and dimension_code not in dataset_json["dimensions_labels"]:
                    # Some dimensions labels are an empty string: e.g. bs_bs12_04.sdmx.xml
                    dataset_json["dimensions_labels"][dimension_code] = dimension_label
            if dimension_code in dataset_json["dimensions_values_labels"] and \
                    dimension_value_code in dataset_json["dimensions_values_labels"][dimension_code]:
                continue
            codelist_code = dsd_infos["codelist_by_concept"][dimension_code]
            dimension_value_label = get_in([codelist_code, dimension_value_code], dsd_infos["codelists"])
            if dimension_value_label:
                dataset_json["dimensions_values_labels"].setdefault(
                    dimension_code, {})[dimension_value_code] = dimension_value_label

        timings["series_labels"] += time.time() - t0

        # Series code is not defined by provider: create it from dimensions values codes.
        series_code = ".".join(
            series_element_attributes[dimension_code]
            for dimension_code in dimensions_codes_order
        )

        # Write series JSON to file.

        t0 = time.time()

        observations_header = [["PERIOD", "VALUE"] + dsd_infos["attributes"]]
        series_json = {
            "code": series_code,
            "dimensions": [
                series_element_attributes[dimension_code]  # Every dimension MUST be defined for each series.
                for dimension_code in dimensions_codes_order
            ],
            "observations": observations_header + dataset_context["current_series_observations"],
        }

        dataset_context["observations_offsets"][series_code] = series_jsonl_file.tell()

        json.dump(series_json, series_jsonl_file, ensure_ascii=False, sort_keys=True)
        series_jsonl_file.write("\n")

        timings["series_file"] += time.time() - t0

        # Reset context for next series.

        dataset_context["current_series_observations"] = []

    elif element.tag.endswith("Obs"):

        # Fill observations attributes labels in dataset.json.

        t0 = time.time()

        for attribute_code, attribute_value_code in element.attrib.items():
            # Ignore period and value observations XML attributes, because they don't need labels.
            if attribute_code in ["TIME_PERIOD", "OBS_VALUE"]:
                continue
            attribute_label = dsd_infos["concepts"].get(attribute_code)
            if attribute_label and attribute_code not in dataset_json["attributes_labels"]:
                dataset_json["attributes_labels"][attribute_code] = attribute_label
            # Some attributes values codes are multi-valued and concatenated into the same string.
            attribute_value_codes = list(attribute_value_code) \
                if attribute_code == "OBS_STATUS" \
                else [attribute_value_code]
            for attribute_value_code in attribute_value_codes:
                if attribute_code in dataset_json["attributes_values_labels"] and \
                        attribute_value_code in dataset_json["attributes_values_labels"][attribute_code]:
                    continue
                codelist_code = dsd_infos["codelist_by_concept"][attribute_code]
                attribute_value_label = get_in([codelist_code, attribute_value_code], dsd_infos["codelists"])
                if attribute_value_label:
                    dataset_json["attributes_values_labels"].setdefault(
                        attribute_code, {})[attribute_value_code] = attribute_value_label

        timings["observations_labels"] += time.time() - t0

        obs_value = element.attrib.get("OBS_VALUE")
        if obs_value is not None:
            obs_value = observations.value_to_float(obs_value)
        dataset_context["current_series_observations"].append([
            element.attrib["TIME_PERIOD"],  # SDMX periods are already normalized.
            obs_value,
        ] + [
            element.attrib.get(attribute_name, "")
            for attribute_name in dsd_infos["attributes"]
        ])


def convert_sdmx_file(dataset_json_stub, sdmx_file: Path, dataset_dir: Path):
    global timings
    timings = {
        k: 0
        for k in {"series_labels", "series_file", "observations_labels", "dsd_infos"}
    }

    assert dataset_json_stub.get("name"), dataset_json_stub
    assert dataset_dir.is_dir(), dataset_dir

    dataset_code = dataset_json_stub["code"]

    # Load DSD
    dsd_file_path = args.source_dir / datasets_dir_name / dataset_code / "{}.dsd.xml".format(dataset_code)
    dsd_element = etree.parse(str(dsd_file_path)).getroot()

    # Initialize dataset.json data

    dataset_json = {
        "attributes_labels": {},  # Will be defined by each series.
        "attributes_values_labels": {},  # Will be defined by each series.
        "dimensions_codes_order": None,  # Will be defined by first series.
        "dimensions_labels": {},  # Will be defined by each series.
        "dimensions_values_labels": {},  # Will be defined by each series.
    }
    dataset_json.update(dataset_json_stub)

    t0 = time.time()

    dsd_infos = {
        "attributes": [
            element.attrib["conceptRef"]
            for element in dsd_element.iterfind(".//{*}Attribute[@attachmentLevel='Observation']")
        ],
        "codelists": {
            element.attrib["id"]: {
                code_element.attrib["value"]: code_element.findtext(
                    "./{*}Description[@xml:lang='en']", namespaces=namespace_url_by_name)
                for code_element in element.iterfind("./{*}Code")
            }
            for element in dsd_element.iterfind('.//{*}CodeList')
        },
        "concepts": {
            element.attrib["id"]: element.findtext("./{*}Name[@xml:lang='en']", namespaces=namespace_url_by_name)
            for element in dsd_element.iterfind('.//{*}Concept')
        },
        "codelist_by_concept": {
            element.attrib["conceptRef"]: element.attrib["codelist"]
            for element in dsd_element.find(".//{*}Components")
            if "conceptRef" in element.attrib and "codelist" in element.attrib
        },
    }

    timings["dsd_infos"] += time.time() - t0

    with (dataset_dir / SERIES_JSONL_FILE_NAME).open("w") as series_jsonl_file:
        dataset_context = {
            "current_series_observations": [],
            "observations_offsets": {},
        }

        # Side-effects: mutate dataset_context, write files.
        context = etree.iterparse(str(sdmx_file), events=["end"])
        for event, element in context:
            convert_sdmx_element(element, dataset_json, dataset_context, dsd_infos, series_jsonl_file)
            if event == "end":
                # Inspired from fast_iter, cf https://www.ibm.com/developerworks/xml/library/x-hiperfparse/
                element.clear()
                while element.getprevious() is not None:
                    del element.getparent()[0]
                continue
        del context

        if dataset_context["observations_offsets"]:
            sqlite_file_path = args.sqlite_dir / "{}.sqlite".format(dataset_code)
            if sqlite_file_path.is_file():
                sqlite_file_path.unlink()
            conn = sqlite3.connect(str(sqlite_file_path))
            cursor = conn.cursor()
            cursor.execute(indexes.SQL_CREATE_TABLE)
            cursor.execute(indexes.SQL_BEGIN_TRANSACTION)
            cursor.executemany(indexes.SQL_INSERT_VALUES, dataset_context["observations_offsets"].items())
            cursor.execute(indexes.SQL_CREATE_INDEX)
            conn.commit()
            conn.close()

        write_json_file(dataset_dir / "dataset.json", without_falsy_values(dataset_json))

    log.debug("timings: {} total: {:.3f}".format(valmap("{:.3f}".format, timings), sum(timings.values())))


def toc_to_category_tree(xml_element, dataset_json_stubs, leaf_index):
    """
    Note: leaf_index is a singleton list because the function parameter must be modified between recursive calls.
    """
    xml_element_tag = xml_element.tag[len("urn:eu.europa.ec.eurostat.navtree") + 2:]
    if xml_element_tag == "tree":
        return list(filter(None, (
            toc_to_category_tree(child_element, dataset_json_stubs, leaf_index)
            for child_element in xml_element
        )))
    elif xml_element_tag == "branch":
        children = list(filter(None, (
            toc_to_category_tree(child_element, dataset_json_stubs, leaf_index)
            for child_element in xml_element.iterfind("{*}children/*")
        )))
        return without_falsy_values({
            "code": xml_element.findtext("{*}code"),
            "name": xml_element.findtext("{*}title[@language='en']"),
            "children": children,
        }) if children else None
    elif xml_element_tag == "leaf" and xml_element.attrib["type"] == "dataset":
        dataset_code = xml_element.findtext("{*}code")
        dataset_name = xml_element.findtext("{*}title[@language='en']")
        leaf_index[0] += 1
        if (args.datasets is None or dataset_code in args.datasets) and \
                (args.exclude_datasets is None or dataset_code not in args.exclude_datasets) and \
                (args.start_from is None or dataset_code == args.start_from):
            if args.start_from is not None:
                # Once "start_from" dataset has been reached, do not stop at the next one.
                args.start_from = None
            if dataset_code not in dataset_json_stubs:
                dataset_json_stubs.append({
                    "code": dataset_code,
                    "name": dataset_name,
                    "description": xml_element.findtext("{*}shortDescription[@language='en']") or None,
                    "doc_href": xml_element.findtext("{*}metadata[@format='html']") or None,
                })
                return {
                    "code": dataset_code,
                    "name": dataset_name,
                }
    return None


def main():
    global args
    global timings
    parser = argparse.ArgumentParser()
    parser.add_argument('source_dir', type=Path,
                        help='path of source directory containing Eurostat series in source format')
    parser.add_argument('target_dir', type=Path, help='path of target directory containing datasets & '
                        'series in DBnomics JSON and TSV formats')
    parser.add_argument('sqlite_dir', type=Path, help='directory to store SQLite indexes for observations')
    parser.add_argument('--datasets', nargs='+', metavar='DATASET_CODE', help='convert only the given datasets')
    parser.add_argument('--exclude-datasets', nargs='+', metavar='DATASET_CODE',
                        help='do not convert the given datasets')
    parser.add_argument('--full', action='store_true',
                        help='convert all datasets; default behavior is to convert what changed since last commit')
    parser.add_argument('--no-commit', action='store_true', help='do not commit at the end of the script')
    parser.add_argument('--resume', action='store_true', help='do not process already written datasets')
    parser.add_argument('--start-from', metavar='DATASET_CODE', help='start indexing from dataset code')
    parser.add_argument('-v', '--verbose', action='store_true', help='display logging messages from debug level')
    args = parser.parse_args()

    if not args.source_dir.is_dir():
        parser.error("Could not find directory {!r}".format(str(args.source_dir)))
    if not args.target_dir.is_dir():
        parser.error("Could not find directory {!r}".format(str(args.target_dir)))
    if not args.sqlite_dir.is_dir():
        parser.error("Could not find directory {!r}".format(str(args.sqlite_dir)))

    logging.basicConfig(
        format="%(levelname)s:%(asctime)s:%(message)s",
        level=logging.DEBUG if args.verbose else logging.INFO,
    )

    log.info("Mode: %s", "full" if args.full else "incremental")

    # Parse "table_of_contents", abbreviated "toc".
    toc_element = etree.parse(str(args.source_dir / "table_of_contents.xml")).getroot()

    # Walk recursively in table_of_contents.xml and return category_tree_json.
    # Side-effects: fill dataset_json_stubs.
    dataset_json_stubs = []
    category_tree_json = toc_to_category_tree(toc_element, dataset_json_stubs, leaf_index=[0])

    # Ask Git which datasets directories were modified in latest commit in source-data repository.
    if not args.full:
        modified_datasets_codes = set(
            dir_name[len("data/"):].strip()
            for dir_name in StringIO(subprocess.check_output(
                "git diff --name-only HEAD^ {} | xargs -n 1 dirname | sort | uniq".format(datasets_dir_name),
                shell=True, cwd=str(args.source_dir), universal_newlines=True))
        )
        log.info("%d datasets have been modified by last download", len(modified_datasets_codes))

    # Convert SDMX files. Side-effect: write files for each dataset.
    converted_datasets_codes = set()
    for index, dataset_json_stub in enumerate(dataset_json_stubs, start=1):
        dataset_code = dataset_json_stub["code"]
        if dataset_code in converted_datasets_codes:
            log.debug("Skipping dataset %r because it was already converted", dataset_code)
            continue

        if not args.full and dataset_code not in modified_datasets_codes:
            log.debug("Skipping dataset %r because it was not modified by last download (due to incremental mode)",
                      dataset_code)
            continue

        source_dataset_dir = args.source_dir / datasets_dir_name / dataset_code
        if not source_dataset_dir.is_dir():
            log.error("Dataset directory %s is missing, skipping", source_dataset_dir)
            continue

        sdmx_file = source_dataset_dir / "{}.sdmx.xml".format(dataset_code)
        if not sdmx_file.is_file():
            log.error("SDMX file %s is missing, skipping", sdmx_file)
            continue

        dataset_dir = args.target_dir / dataset_code
        if args.resume:
            if (dataset_dir / "dataset.json").is_file():
                log.debug("Skipping dataset %r because it already exists (due to --resume option)", dataset_code)
                continue
        elif dataset_dir.is_dir():
            shutil.rmtree(str(dataset_dir))

        log.info("Converting SDMX source file %d/%d %s (%s)", index, len(dataset_json_stubs), sdmx_file,
                 humanize.naturalsize(sdmx_file.stat().st_size, gnu=True))

        dataset_dir.mkdir(exist_ok=True)
        convert_sdmx_file(dataset_json_stub, sdmx_file, dataset_dir)
        converted_datasets_codes.add(dataset_code)

    write_json_file(args.target_dir / "provider.json", provider_json)
    if category_tree_json:
        write_json_file(args.target_dir / "category_tree.json", category_tree_json)

    return 0


def without_falsy_values(mapping):
    return {
        k: v
        for k, v in mapping.items()
        if v
    }


def write_json_file(path, data):
    with path.open("w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)


if __name__ == '__main__':
    sys.exit(main())
