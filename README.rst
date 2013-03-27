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

Example code usage::

    from datatap.dataps import JSONStreamDataTap, ModelDataTap, ZipFileDataTap
    
    #with django models
    outstream = JSONStreamDataTap(stream=sys.stdout)
    outstream.open('w'. for_datatap=ModelDataTap)
    source = ModelDataTap(MyModel, User.objects.filter(is_active=True))
    source.dump(outstream)
    
    instream = JSONStreamDataTap(stream=open('fixture.json', 'r'))
    ModelDataTap.load(instream)
    
    #give me all active users to stdout
    ModelDataTap.store(JSONStreamDataTap(stream=sys.stdout), User.objects.filter(is_active=True))
    
    #write Blog and BlogImages to a zipfile
    archive = ZipFileDataTap(filename='myblog.zip')
    archive.open('w', for_datatap=ModelDataTap)
    #or do it in one line: archive = ZipFileDataTap(filename='myblog.zip', mode='w', for_datatap=ModelDataTap)
    ModelDataTap.store(archive, Blog, BlogImages)
    archive.close()
    
    archive = ZipFileDataTap(filename='myblog.zip', mode='r')
    ModedDataTap.load(archive)

Datatap includes a management command to allow dumping and loading to particular data stores (zip file, json file, S3, etc). Some datataps include the originating data tap so that the resulting data store can be automatically detected.

Format::

    manage.py datatap <source> <source vargs> -- <destination> <destination vargs>
    manage.py datatap <source> <source vargs>

Example command line usage::

    manage.py datatap Model app1 app2 app3.model -- ZipFile --file=myfile.zip
    
    #3rd party can register their own data taps
    manage.py datatap DocKitCMS --app=customapp1 --app=customapp2 --collection=blog --publicresource=myblog > objects.json
    
    #destination data tap is autodetected
    manage.py datatap ZipFile --file=myfile.zip


