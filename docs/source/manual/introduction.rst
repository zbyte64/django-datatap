.. _introduction:

============
Introduction
============

Datataps are classes able to serialize and deserialize objects in their domain. A datatap maybe chained with another to provide serialization to a particular format or for objects to be read from a general data source like a zip file. Datataps also handle the serialization and deserialization of django File objects within the native objects allowing for assets to follow the application data.

Datatap includes a management command to allow dumping and loading to particular data stores (zip file, json file, S3, etc). 

TODO: Some datataps include the originating data tap so that the resulting data store can be automatically detected.
