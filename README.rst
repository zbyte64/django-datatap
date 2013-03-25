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

Events are emitted from hyperadmin resources when actions are completed. These events are listened to by Subcribers in eventsocket. These objects serialize the message and fowards it to a publisher. A publisher executes a scheduled message to a data source. The task may run the message through a transformation function before sending.

This would allow for new objects to be posted to a CRM via a webhook or a metric to elasticsearch. It may even goto a redis store to power a pub sub.

There should be an admin panel for chaining these objects and saving them in a db. Due to the dynamic nature of these objects something like django-configstore can handle the variable forms and serialization.
