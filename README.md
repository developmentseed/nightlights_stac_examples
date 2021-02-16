## Example hierarchy
```
                                catalog.json
                                      |
                              VIIRS_npp.json
                                      |
                             201204_catalog.json
                                      |
SVDNB_npp_d20120401_t0009017_e0014421_b02208_c20120428182646408538_devl_pop.rade9.co.json
```

(The item json also references VIIRS_npp as it is part of the collection).

## A few notes on  best practices / errors

Use absolute links when possible.

Child catalogs should be specified with the `child` link rather than the `item` link.

The examples I've provided and your item are currently using the bare minimum for properties and extensions.  I'd recommend investigating the available extensions and seeing what might be valuable to your data consumers.

You'll note that I have loosely modeled the VIIRS and DMSP data as [collections](https://github.com/radiantearth/stac-spec/tree/master/collection-spec).  The intent of collections is a semantic model for groups of items with common metadata properties, in this case the 2 sensors.  I have not included example properties but they are a requirement.

I quickly validated the individual structures here using [stac-validator](https://github.com/sparkgeo/stac-validator).  It is invaluable for verifying the validity of your structures and links.

Once you feel you have a solid initial cut of your data hierarchy, please reach out and I will try to organize a review with some other folks from the STAC community so they can provide feedback about other options for the data model.


We saw a few messages on the STAC Gitter from people attempting to leverage the catalog file you published.  There are several fixes/step you'll need to take to get this to a minimum functioning catalog.  What I've provided here is just a rough example, for the actual public deployment you will need.
1. The root catalog.json file should be updated to remove the example DMSP collection I included or your can build a DMSP collection as well.  All of the links in your Catalogs, Collections and Items should be valid and the files themselves should be validated via stac-validator or during construction via PySTAC validation.

2.  You will want to expand the VIIRS collection file to include the [summaries](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md) that are common to the VIIRS platform.  Though not required these summaries properties are a good practice. Here is a nice [example](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/examples/sentinel2.json)

3. The 201204_catalog.json I included is just a reference example and I have removed all of its items but one for demonstration purposes.  All of you monthyly sub-catalogs will need to be updated to have the correct link structure and be re-written to S3.  I would strongly suggest using PySTAC to manage this.

4. Additionally, the link structure of all your items will need to be corrected to include the correct root, parent and collection links.  Again I'd suggest PySTAC for managing this.

5. After your entire catalog structure has been published it will need to be ingested into an API in order to be searchable by temporal and spatial characteristics.  Depending upon the API implementation, updates to existing items can be difficult.  Because of this I would suggest reviewing all of the STAC extensions which might be relative to your data and implementing them.  It is often easier to ingest a complete item into the API rather than ingest a partial one and update it later.  PySTAC supports the majority of these extensions and can handle validation for you.

6. In order to utilize this catalog with static catalog crawlers and browsers you will need to enable a CORS policy on your bucket as a best [practice](https://github.com/radiantearth/stac-spec/issues/888).
