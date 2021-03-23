from urllib.parse import urlparse, urlunparse, urljoin
import requests
from pystac import STAC_IO, Item, Asset, Link, MediaType
from pystac.extensions.eo import Band
from datetime import datetime
import boto3
import json
from iteration_utilities import groupedby

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


def add_assets(item, root_url, segment, creation_stamp):
    item.ext.enable("eo")

    dnb_asset = Asset(
        href=urljoin(
            f"{root_url}/",
            f"SVDNB_{segment}_{creation_stamp}_noaa_ops.rade9.co.tif"
        ),
        media_type=MediaType.COG,
        roles=["data"]
    )
    dnb_bands = [
        Band.create(
            name="DNB",
            common_name="day night band",
            center_wavelength=0.7,
            full_width_half_max=0.4
        )
    ]
    item.ext.eo.set_bands(dnb_bands, dnb_asset)
    item.add_asset("DNB", dnb_asset)

    m15_asset = Asset(
        href=urljoin(
            f"{root_url}/",
            f"SVM15_{segment}_{creation_stamp}_noaa_ops.rad.co.tif"
        ),
        media_type=MediaType.COG,
        roles=["data"]
    )
    m15_bands = [
        Band.create(
            name="M15",
            common_name="thermal infrared",
            center_wavelength=10.763,
            full_width_half_max=1
        )
    ]
    item.ext.eo.set_bands(m15_bands, m15_asset)
    item.add_asset("M15", m15_asset)

    vflag_asset = Asset(
        href=urljoin(
            f"{root_url}/",
            f"{segment}_vflag.co.tif"
        ),
        media_type=MediaType.COG,
        roles=["data-mask"]
    )
    item.add_asset("VFLAG", vflag_asset)

    gdnbo_asset = Asset(
        href=urljoin(
            f"{root_url}/",
            f"GDNBO_{segment}_noaa_ops.li.co.tif"
        ),
        media_type=MediaType.COG,
        roles=["data"]
    )
    item.add_asset("GDNBO", gdnbo_asset)

    geolocation_sample_asset = Asset(
        href=urljoin(
            f"{root_url}/",
            f"GDNBO_{segment}_noaa_ops.samples.co.tif"
        ),
        media_type=MediaType.COG,
        roles=["data"]
    )
    item.add_asset("geolocation_sample", geolocation_sample_asset)


def add_links(item, root_url, segment):
    item.set_self_href(
        urljoin(
            f"{root_url}/",
            f"{segment}.json"
        )
    )

    parent_link = Link(
        "parent",
        target=urljoin(
            f"{root_url}/",
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

s3 = boto3.client('s3')
paginator = s3.get_paginator('list_objects_v2')
response_iterator = paginator.paginate(
    Bucket="globalnightlight",
    Prefix="npp_202008"
)

objects = []
for response in response_iterator:
    objects = objects + response["Contents"]


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


dct = groupedby(objects, key=get_segment, keep=get_key)
for key in dct.keys():
    print(key)
    if key is not None:
        print(dct[key])

# item_url = "https://globalnightlight.s3.amazonaws.com/npp_202008/SVDNB_npp_d20200801_t0048355_e0054159_b45395_c20200801045415683718_noac_ops.rade9.co.json"
# parsed_url = urlparse(item_url)
# root_key = parsed_url.path.split("/")[1]
# root_url = urlunparse((
    # parsed_url.scheme,
    # parsed_url.netloc,
    # root_key,
    # None,
    # None,
    # None
# ))

# existing_item = Item.from_file(item_url)
# id_components = existing_item.id.split("_")
# orbital_segment = f"{id_components[1]}_{id_components[2]}_" \
    # f"{id_components[3]}_{id_components[4]}_{id_components[5]}"
# creation_stamp = id_components[6]

# start_time = datetime.strptime(
    # f"{id_components[2][1:]}{id_components[3][1:]}",
    # "%Y%m%d%H%M%S%f"
# )
# creation_time = datetime.strptime(
    # creation_stamp[1:],
    # "%Y%m%d%H%M%S%f"
# )
# new_item = Item(
    # id=orbital_segment,
    # bbox=existing_item.bbox,
    # geometry=existing_item.geometry,
    # datetime=start_time,
    # properties={},
# )
# end_time = datetime.strptime(
    # f"{id_components[2][1:]}{id_components[4][1:]}",
    # "%Y%m%d%H%M%S%f"
# )
# new_item.common_metadata.start_datetime = start_time
# new_item.common_metadata.end_datetime = end_time
# new_item.common_metadata.created = creation_time
# new_item.common_metadata.gsd = 750
# new_item.common_metadata.platform = "s-npp"
# new_item.common_metadata.instruments = ["viirs"]

# add_assets(new_item, root_url, orbital_segment, creation_stamp)
# add_links(new_item, root_url, orbital_segment)
# new_item.validate()
# with open(f"{orbital_segment}.json", 'w') as outfile:
    # json.dump(new_item.to_dict(), outfile)
# print(json.dumps(new_item.to_dict()))
