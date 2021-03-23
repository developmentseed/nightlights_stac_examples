from urllib.parse import urlparse, urlunparse, urljoin
import requests
from pystac import STAC_IO, Catalog, Link, Item, Asset, MediaType
from pystac.extensions.eo import Band
from datetime import datetime
import boto3
import json


def http_read_method(uri):
    parsed = urlparse(uri)
    if parsed.scheme.startswith('http'):
        return requests.get(uri).text
    else:
        return STAC_IO.default_read_text_method(uri)


def s3_write_method(uri, txt):
    parsed = urlparse(uri)
    if parsed.scheme.startswith('http'):
        bucket = parsed.netloc.replace('.s3.amazonaws.com', '')
        key = parsed.path[1:]
        s3 = boto3.resource("s3")
        s3.Object(bucket, key).put(Body=txt, ContentType='application/json')
    else:
        STAC_IO.default_write_text_method(uri, txt)


def add_assets(item, root_url, segment, creation_stamp, bands):
    item.ext.enable('eo')
    svdnb = f'SVDNB_{segment}_{creation_stamp}_noaa_ops.rade9.co.tif'
    href = urljoin(f'{root_url}/', svdnb)
    svdnb_asset = Asset(
        href=href,
        media_type=MediaType.COG,
        roles=["data"]
    )
    bands = [
        Band.create(
            name='DNB',
            common_name='day night band',
            center_wavelength=0.7,
            full_width_half_max=0.4
        )
    ]
    item.ext.eo.set_bands(bands, svdnb_asset)
    item.add_asset('SVDNB', svdnb_asset)


STAC_IO.read_text_method = http_read_method
STAC_IO.write_text_method = s3_write_method


item_url = 'https://globalnightlight.s3.amazonaws.com/npp_202008/SVDNB_npp_d20200801_t0048355_e0054159_b45395_c20200801045415683718_noac_ops.rade9.co.json'
parsed_url = urlparse(item_url)
root_key = parsed_url.path.split('/')[1]
root_url = urlunparse((
    parsed_url.scheme,
    parsed_url.netloc,
    root_key,
    None,
    None,
    None
))

existing_item = Item.from_file(item_url)
id_components = existing_item.id.split('_')
orbital_segment = f'{id_components[1]}_{id_components[2]}_' \
    f'{id_components[3]}_{id_components[4]}_{id_components[5]}'
creation_stamp = id_components[6]

start_time = datetime.strptime(
    f'{id_components[2][1:]}{id_components[3][1:]}',
    '%Y%m%d%H%M%S%f'
)
new_item = Item(
    id=orbital_segment,
    bbox=existing_item.bbox,
    geometry=existing_item.geometry,
    datetime=start_time,
    properties={},
)
end_time = datetime.strptime(
    f'{id_components[2][1:]}{id_components[4][1:]}',
    '%Y%m%d%H%M%S%f'
)
new_item.common_metadata.start_datetime = start_time
new_item.common_metadata.end_datetime = end_time
new_item.common_metadata.gsd = 750

bands = {
    "M15": {
        "band": Band.create(
            name="M15",
            common_name="longwave infrared",
            center_wavelength=10.763,
            full_width_half_max=1,
        ),
        "gsd": 750.0
    },
    "DNB": {
        "band": Band.create(
            name="DNB",
            common_name="day night band",
            center_wavelength=0.7,
            full_width_half_max=0.4
        ),
        "gsd": 750.0
    }
}
add_assets(new_item, root_url, orbital_segment, creation_stamp, bands)
new_item.validate()
print(json.dumps(new_item.to_dict()))
