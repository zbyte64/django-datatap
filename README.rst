.. image:: https://secure.travis-ci.org/zbyte64/django-datatap.png?branch=master
   :target: http://travis-ci.org/zbyte64/django-datatap


============
Introduction
============

django-datatap is a fixture system enabling applications to define their own loading and dumping process while supporting file asset storage.

Documentation: https://django-datatap.rtfd.org/

------------
Requirements
------------

* Python 2.6 or later
* Django 1.3 or later


============
Installation
============

Put 'datatap' into your ``INSTALLED_APPS`` section of your settings file.


=======
Concept
=======

Datataps are classes able to serialize and deserialize objects in their domain. A datatap maybe chained with another to provide serialization to a particular format or for objects to be read from a general data source like a zip file. Datataps also handle the serialization and deserialization of django File objects within the native objects allowing for assets to follow the application data.


Datatap includes a management command to allow dumping and loading to particular data stores (zip file, json file, S3, etc). Some datataps include the originating data tap so that the resulting data store can be automatically detected.

===============
Datatap Command
===============

Chain a series of datataps with the source starting at the left and the right most to write. Each datatap invocation is seperated by "--"

Format::

    manage.py datatap <datataptype> <datatap vargs> [(-- <datataptype> <datatap vargs>), ...] (-- <destination datataptype> <datatap vargs>)

Example command line usage::

    manage.py datatap Model contenttypes -- Zip -- File archive.zip
    
    manage.py datatap File archive.zip -- Zip -- Model
    
    #3rd party apps can register their own data taps
    manage.py datatap DocKitCMS --app=customapp1 --app=customapp2 --collection=blog --publicresource=myblog -- JSON -- Stream > objects.json

