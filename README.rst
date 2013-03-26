============
Introduction
============

django-datatap is a fixture system enabling applications to define their own fixture loading and dumping process.

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

Example usage:

    from datatap.dataps import JSONStreamDataTap, ModelDataTap, ResourceDataTap
    
    #with django models
    outstream = JSONStreamDataTap(stream=sys.stdout)
    outstream.open('w')
    ModelDataTap.store(outstream, MyModel, User.objects.filter(is_active=True))
    outstream.close()
    
    instream = JSONStreamDataTap(stream=open('fixture.json', 'r'))
    ModelDataTap.load(instream)
    
    
    #with hyperadmin resources
    outstream = JSONStreamDataTap(stream=sys.stdout)
    outstream.open('w')
    ResourceDataTap.store(outstream, MyResource)
    outstream.close()
    
    instream = JSONStreamDataTap(stream=open('fixture.json', 'r'))
    ResourceDataTap.load(instream)
    
    #or with substitutions
    instream = JSONStreamDataTap(stream=open('fixture.json', 'r'))
    ResourceDataTap.load(instream, mapping={'myresource_resource':'target_resource'})

Further development will include a management command to allow dumping and loading to particular data stores (zip file, json file, S3, etc). It is likely the serialization format will change to include the originating data tap so that the resulting data store can be automatically detected.

Possible command line usage:    

    manage.py dumpdatatap Model app1 app2 app3.model -- ZipFile --file=myfile.zip
    manage.py dumpdatatap <source> <source vargs> -- <destination> <destination vargs>
    
    manage.py loaddatatap ZipFile myfile.zip
    manage.py loaddatatap <source> <source vargs>
    
    manage.py dumpdatatap DocKitCMS --app=customapp1 --app=customapp2 --collection=blog --publicresource=myblog > objects.json
    manage.py dumpdatatap <source> <source vargs>
