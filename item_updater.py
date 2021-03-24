from urllib.parse import urlparse, urljoin
import requests
from pystac import STAC_IO, Item, Asset, Link, MediaType
from pystac.extensions.eo import Band
from datetime import datetime
import boto3
import json
from iteration_utilities import groupedby
from os.path import splitext

bucket_path = "https://globalnightlight.s3.amazonaws.com"


def http_read_method(uri):
    parsed = urlparse(uri)
    if parsed.scheme.startswith("http"):
        return requests.get(uri).text
    else:
        return STAC_IO.default_read_text_method(uri)


def s3_write_method(uri, txt):
    parsed = urlparse(uri)
    if parsed.scheme.startswith("http"):
        bucket = parsed.netloc.replace(".s3.amazonaws.com", "")
        key = parsed.path[1:]
        s3 = boto3.resource("s3")
        s3.Object(bucket, key).put(Body=txt, ContentType="application/json")
    else:
        STAC_IO.default_write_text_method(uri, txt)


def add_dnb_asset(item, href, asset_key):
    asset = Asset(
        href=href,
        media_type=MediaType.COG,
        roles=["data"]
    )
    dnb_bands = [
        Band.create(
            name=asset_key,
            common_name="day night band",
            center_wavelength=0.7,
            full_width_half_max=0.4
        )
    ]
    item.ext.eo.set_bands(dnb_bands, asset)
    item.add_asset(asset_key, asset)


def add_m15_asset(item, href, asset_key):
    asset = Asset(
        href=href,
        media_type=MediaType.COG,
        roles=["data"]
    )
    m15_bands = [
        Band.create(
            name=asset_key,
            common_name="thermal infrared",
            center_wavelength=10.763,
            full_width_half_max=1
        )
    ]
    item.ext.eo.set_bands(m15_bands, asset)
    item.add_asset(asset_key, asset)


def add_asset(item, href, asset_key):
    asset = Asset(
        href=href,
        media_type=MediaType.COG,
        roles=["data"]
    )
    item.add_asset(asset_key, asset)


def parse_creation_date(segment_file):
    components = segment_file.split("_")
    creation_time = datetime.strptime(
        components[6][1:],
        "%Y%m%d%H%M%S%f"
    )
    return creation_time


def process_segment_files(item, prefix_url, segment_files):
    item.ext.enable("eo")
    for segment_file in segment_files:
        href = urljoin(f"{prefix_url}/", segment_file)
        components = segment_file.split("_")
        if components[0] == "SVDNB":
            name, ext = splitext(segment_file)
            if ext == ".tif":
                key = "DNB"
                add_dnb_asset(item, href, key)
                date = parse_creation_date(segment_file)
                item.common_metadata.set_created(date, item.assets[key])

        if components[0] == "SVM15":
            key = "M15"
            add_m15_asset(item, href, key)
            date = parse_creation_date(segment_file)
            item.common_metadata.set_created(date, item.assets[key])

        if components[0] == "GDNBO" or components[0] == "GDTCN":
            if "samples" in segment_file:
                key = "SAMPLE"
                add_asset(item, href, key)
            else:
                key = "LI"
                add_asset(item, href, key)
            date = parse_creation_date(segment_file)
            item.common_metadata.set_created(date, item.assets[key])


def add_links(item, prefix_url, segment):
    item.set_self_href(
        urljoin(
            f"{prefix_url}/",
            f"{segment}.json"
        )
    )

    parent_link = Link(
        "parent",
        target=urljoin(
            f"{prefix_url}/",
            "catalog.json"
        ),
        media_type=MediaType.JSON
    )
    item.add_link(parent_link)

    root_link = Link(
        "root",
        target=urljoin(
            f"{bucket_path}/",
            "catalog.json"
        ),
        media_type=MediaType.JSON
    )
    item.add_link(root_link)


STAC_IO.read_text_method = http_read_method
STAC_IO.write_text_method = s3_write_method


prefix = "npp_202008"
s3 = boto3.client('s3')
paginator = s3.get_paginator('list_objects_v2')
response_iterator = paginator.paginate(
    Bucket="globalnightlight",
    Prefix=prefix
)


def get_segment(obj):
    key = obj["Key"]
    components = key.split("/")[1].split("_")
    if len(components) > 6:
        segment = f"{components[1]}_{components[2]}_" \
            f"{components[3]}_{components[4]}_{components[5]}"
    else:
        segment = None
    return segment


def get_key(obj):
    key = obj["Key"]
    return key.split("/")[1]


def get_existing_item(prefix_url, segment_files):
    filtered = [
        segment_file for segment_file in segment_files
        if splitext(segment_file)[1] == ".json"
    ]
    item_url = urljoin(f"{prefix_url}/", filtered[0])
    existing_item = Item.from_file(item_url)
    return existing_item


def create_item(segment, segment_files, prefix_url):
    segment_components = segment.split("_")

    start_time = datetime.strptime(
        f"{segment_components[1][1:]}{segment_components[2][1:]}",
        "%Y%m%d%H%M%S%f"
    )
    existing_item = get_existing_item(prefix_url, segment_files)
    new_item = Item(
        id=segment,
        bbox=existing_item.bbox,
        geometry=existing_item.geometry,
        datetime=start_time,
        properties={},
    )
    end_time = datetime.strptime(
        f"{segment_components[1][1:]}{segment_components[3][1:]}",
        "%Y%m%d%H%M%S%f"
    )
    new_item.common_metadata.start_datetime = start_time
    new_item.common_metadata.end_datetime = end_time
    new_item.common_metadata.gsd = 750
    new_item.common_metadata.platform = "s-npp"
    new_item.common_metadata.instruments = ["viirs"]

    process_segment_files(new_item, prefix_url, segment_files)
    vflag_href = urljoin(
        f"{prefix_url}/",
        f"{segment}.co.tif"
    )
    add_asset(new_item, vflag_href, "VFLAG")
    add_links(new_item, prefix_url, segment)
    new_item.validate()
    print(json.dumps(new_item.to_dict()))

    # with open(f"{orbital_segment}.json", 'w') as outfile:
        # json.dump(new_item.to_dict(), outfile)


objects = []
for response in response_iterator:
    objects = objects + response["Contents"]

grouped_objects = groupedby(objects, key=get_segment, keep=get_key)

for key in grouped_objects.keys():
    print(key)
    if key is not None:
        url = urljoin(bucket_path, prefix)
        create_item(key, grouped_objects[key], url)
