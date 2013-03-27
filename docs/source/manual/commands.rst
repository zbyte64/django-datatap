Commands
========

dumpdatatap
-----------

Format::

    manage.py dumpdatatap <source> <source vargs> -- <destination> <destination vargs>
    manage.py dumpdatatap <source> <source vargs>


When no destination is specified it will be autodected from the source or a JSON standard out stream will be used.


Example command line usage::

    manage.py dumpdatatap Model app1 app2 app3.model -- ZipFile --file=myfile.zip
    
    #3rd party can register their own data taps
    manage.py dumpdatatap DocKitCMS --app=customapp1 --app=customapp2 --collection=blog --publicresource=myblog > objects.json


loaddatatap
-----------

Format::

    manage.py loaddatatap <source> <source vargs>

Example command line usage::

    manage.py loaddatatap ZipFile --file=myfile.zip
    
    #which is the same as
    manage.py dumpdatatap ZipFile --file=myfile.zip -- Model
