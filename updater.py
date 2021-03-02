from urllib.parse import urlparse
import requests
from pystac import STAC_IO, Catalog, Link
import boto3

bucket_path = 'https://wb-nightlights.s3.amazonaws.com'
s3_path = 's3://wb-nightlights'


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


STAC_IO.read_text_method = http_read_method
STAC_IO.write_text_method = s3_write_method

catalog = Catalog(
    id='nighttime_visible_radiance_1992-2020',
    description='Nightly visible radiance catalogues from DMSP-OLS (1992-2017, various satellites) and VIIRS NPP (2012-2020).'
)
existing_catalog = Catalog.from_file('https://globalnightlight.s3.amazonaws.com/VIIRS_npp_catalog.json')
catalog.set_self_href(f'{bucket_path}/catalog.json')


for subcat in existing_catalog.get_children():
    if subcat.id == 'VIIRS_DNB_rade9_201204':
        ym = subcat.id.split('_')[-1]
        links = subcat.get_links()
        subcat.remove_links('item')
        for link in links:
            if link.rel == 'item':
                absolute_target = f'https://globalnightlight.s3.amazonaws.com/{ym}/{link.target[2:]}'
                updated_link = Link(
                    'item',
                    target=absolute_target,
                    media_type=link.media_type,
                )
                subcat.add_link(updated_link)
                print(absolute_target)
        subcat.set_self_href(f'{bucket_path}/{ym}/catalog.json')
        subcat.set_root(catalog)
        subcat.set_parent(catalog)
        subcat.save_object(
            include_self_link=True,
        )
        subcat.validate()
        catalog.add_child(subcat)
        break

catalog.validate()
catalog.save_object(
    include_self_link=True,
)
