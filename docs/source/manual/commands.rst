Commands
========

datatap
-------

Chain a series of datataps with the source starting at the left and the right most to write. Each datatap invocation is seperated by "--"

Format::

    manage.py datatap <datataptype> <datatap vargs> [(-- <datataptype> <datatap vargs>), ...] (-- <destination datataptype> <datatap vargs>)


When no destination is specified it will be autodected from the source or a JSON standard out stream will be used.


Example command line usage::

    manage.py datatap Model contenttypes -- Zip -- File archive.zip
    
    manage.py datatap File archive.zip -- Zip -- Model
    
    #3rd party apps can register their own data taps
    manage.py datatap DocKitCMS --app=customapp1 --app=customapp2 --collection=blog --publicresource=myblog -- JSON -- Stream > objects.json

