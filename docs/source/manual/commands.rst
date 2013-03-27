Commands
========

datatap
-------

Format::

    manage.py datatap <source> <source vargs> -- <destination> <destination vargs>
    manage.py datatap <source> <source vargs>


When no destination is specified it will be autodected from the source or a JSON standard out stream will be used.


Example command line usage::

    manage.py datatap Model app1 app2 app3.model -- ZipFile --file=myfile.zip
    
    #3rd party can register their own data taps
    manage.py datatap DocKitCMS --app=customapp1 --app=customapp2 --collection=blog --publicresource=myblog > objects.json
    
    #destination data tap is autodetected
    manage.py datatap ZipFile --file=myfile.zip

