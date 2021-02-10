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
