
Treasure Data API library for Python
====================================


.. image:: https://travis-ci.org/treasure-data/td-client-python.svg
   :target: https://travis-ci.org/treasure-data/td-client-python
   :alt: Build Status


.. image:: https://ci.appveyor.com/api/projects/status/eol91l1ag50xee9m/branch/master?svg=true
   :target: https://ci.appveyor.com/project/treasure-data/td-client-python/branch/master
   :alt: Build status


.. image:: https://coveralls.io/repos/treasure-data/td-client-python/badge.svg
   :target: https://coveralls.io/r/treasure-data/td-client-python
   :alt: Coverage Status


.. image:: https://badge.fury.io/py/td-client.svg
   :target: http://badge.fury.io/py/td-client
   :alt: PyPI version


Treasure Data API library for Python

Requirements
------------

``td-client`` supports the following versions of Python.


* Python 3.5+
* PyPy

Install
-------

You can install the releases from `PyPI <https://pypi.python.org/>`_.

.. code-block:: sh

   $ pip install td-client

It'd be better to install `certifi <https://pypi.python.org/pypi/certifi>`_ to enable SSL certificate verification.

.. code-block:: sh

   $ pip install certifi

Examples
--------

Please see also the examples at `Treasure Data Documentation <http://docs.treasuredata.com/articles/rest-api-python-client>`_.

The td-client documentation is hosted at https://tdclient.readthedocs.io/,
or you can go directly to the
`API documentation <https://tdclient.readthedocs.io/en/latest/api/index.html>`_.

For information on the parameters that may be used when reading particular
types of data, see `File import parameters`_.

.. _`file import parameters`:
   https://tdclient.readthedocs.io/en/latest/api/file_import_paremeters.html

Listing jobs
^^^^^^^^^^^^

Treasure Data API key will be read from environment variable ``TD_API_KEY``\ , if none is given via ``apikey=`` argument passed to ``tdclient.Client``.

Treasure Data API endpoint ``https://api.treasuredata.com`` is used by default. You can override this with environment variable ``TD_API_SERVER``\ , which in turn can be overridden via ``endpoint=`` argument passed to ``tdclient.Client``. List of available Treasure Data sites and corresponding API endpoints can be found `here <https://support.treasuredata.com/hc/en-us/articles/360001474288-Sites-and-Endpoints>`_.

.. code-block:: python

   import tdclient

   with tdclient.Client() as td:
       for job in td.jobs():
           print(job.job_id)

Running jobs
^^^^^^^^^^^^

Running jobs on Treasure Data.

.. code-block:: python

   import tdclient

   with tdclient.Client() as td:
       job = td.query("sample_datasets", "SELECT COUNT(1) FROM www_access", type="hive")
       job.wait()
       for row in job.result():
           print(repr(row))

Running jobs via DBAPI2
^^^^^^^^^^^^^^^^^^^^^^^

td-client-python implements `PEP 0249 <https://www.python.org/dev/peps/pep-0249/>`_ Python Database API v2.0.
You can use td-client-python with external libraries which supports Database API such like `pandas <http://pandas.pydata.org/>`_.

.. code-block:: python

   import pandas
   import tdclient

   def on_waiting(cursor):
       print(cursor.job_status())

   with tdclient.connect(db="sample_datasets", type="presto", wait_callback=on_waiting) as td:
       data = pandas.read_sql("SELECT symbol, COUNT(1) AS c FROM nasdaq GROUP BY symbol", td)
       print(repr(data))

We offer another package for pandas named `pytd <https://github.com/treasure-data/pytd>`_ with some advanced features.
You may prefer it if you need to do complicated things, such like exporting result data to Treasure Data, printing job's
progress during long execution, etc.

Importing data
^^^^^^^^^^^^^^

Importing data into Treasure Data in streaming manner, as similar as `fluentd <http://www.fluentd.org/>`_ is doing.

.. code-block:: python

   import sys
   import tdclient

   with tdclient.Client() as td:
       for file_name in sys.argv[:1]:
           td.import_file("mydb", "mytbl", "csv", file_name)


.. Warning::
   Importing data in streaming manner requires certain amount of time to be ready to query since schema update will be
   executed with delay.

Bulk import
^^^^^^^^^^^

Importing data into Treasure Data in batch manner.

.. code-block:: python

   import sys
   import tdclient
   import uuid
   import warnings

   if len(sys.argv) <= 1:
       sys.exit(0)

   with tdclient.Client() as td:
       session_name = "session-{}".format(uuid.uuid1())
       bulk_import = td.create_bulk_import(session_name, "mydb", "mytbl")
       try:
           for file_name in sys.argv[1:]:
               part_name = "part-{}".format(file_name)
               bulk_import.upload_file(part_name, "json", file_name)
           bulk_import.freeze()
       except:
           bulk_import.delete()
           raise
       bulk_import.perform(wait=True)
       if 0 < bulk_import.error_records:
           warnings.warn("detected {} error records.".format(bulk_import.error_records))
       if 0 < bulk_import.valid_records:
           print("imported {} records.".format(bulk_import.valid_records))
       else:
           raise(RuntimeError("no records have been imported: {}".format(bulk_import.name)))
       bulk_import.commit(wait=True)
       bulk_import.delete()


If you want to import data as `msgpack <https://msgpack.org/>`_ format, you can write as follows:

.. code-block:: python

   import io
   import time
   import uuid
   import warnings

   import tdclient

   t1 = int(time.time())
   l1 = [{"a": 1, "b": 2, "time": t1}, {"a": 3, "b": 9, "time": t1}]

   with tdclient.Client() as td:
       session_name = "session-{}".format(uuid.uuid1())
       bulk_import = td.create_bulk_import(session_name, "mydb", "mytbl")
       try:
           _bytes = tdclient.util.create_msgpack(l1)
           bulk_import.upload_file("part", "msgpack", io.BytesIO(_bytes))
           bulk_import.freeze()
       except:
           bulk_import.delete()
           raise
       bulk_import.perform(wait=True)
       # same as the above example


Changing how CSV and TSV columns are read
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``td-client`` package will generally make sensible choices on how to read
the columns in CSV and TSV data, but sometimes the user needs to override the
default mechanism. This can be done using the optional `file import
parameters`_ ``dtypes`` and ``converters``.

For instance, consider CSV data that starts with the following records::

  time,col1,col2,col3
  1575454204,a,0001,a;b;c
  1575454204,b,0002,d;e;f

If that data is read using the defaults, it will produce values that look
like:

.. code:: python

  1575454204, "a", 1, "a;b;c"
  1575454204, "b", 2, "d;e;f"
  
that is, an integer, a string, an integer and another string.

If the user wants to keep the leading zeroes in ``col2``, then they can
specify the column datatype as string. For instance, using
``bulk_import.upload_file`` to read data from ``input_data``:

.. code:: python

    bulk_import.upload_file(
        "part", "msgpack", input_data,
        dtypes={"col2": "str"},
    )

which would produce:

.. code:: python

  1575454204, "a", "0001", "a;b;c"
  1575454204, "b", "0002", "d;e;f"

If they also wanted to treat ``col3`` as a sequence of strings, separated by
semicolons, then they could specify a function to process ``col3``:

.. code:: python

    bulk_import.upload_file(
        "part", "msgpack", input_data,
        dtypes={"col2": "str"},
        converters={"col3", lambda x: x.split(";")},
    )

which would produce:

.. code:: python

  1575454204, "a", "0001", ["a", "b", "c"]
  1575454204, "b", "0002", ["d", "e", "f"]

Development
-----------

Running tests
^^^^^^^^^^^^^

Run tests.

.. code-block:: sh

   $ python setup.py test

Running tests (tox)
^^^^^^^^^^^^^^^^^^^

You can run tests against all supported Python versions. I'd recommend you to install `pyenv <https://github.com/yyuu/pyenv>`_ to manage Pythons.

.. code-block:: sh

   $ pyenv shell system
   $ for version in $(cat .python-version); do [ -d "$(pyenv root)/versions/${version}" ] || pyenv install "${version}"; done
   $ pyenv shell --unset

Install `tox <https://pypi.python.org/pypi/tox>`_.

.. code-block:: sh

   $ pip install tox

Then, run ``tox``.

.. code-block:: sh

   $ tox

Release
^^^^^^^

Release to PyPI. Ensure you installed twine.

.. code-block:: sh

   $ python setup.py bdist_wheel sdist
   $ twine upload dist/*

License
-------

Apache Software License, Version 2.0
