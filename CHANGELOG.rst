
Version History
===============

Unreleased
----------

v1.2.0 (2019-12-05)
--------------------

* Add new (optional) parameters to ``ImportApi.import_files``,
  ``BulkImportApi.bulk_import_upload_file`` and ``BulkImport.upload_file``. (#85)
  The ``dtypes`` and ``converters`` parameters allow better control of the
  import of CSV data (#83). This is modelled on the approach taken by pandas.
* Ensure ``config`` key for ``ConnectorAPI.connector_guess`` (#84)

v1.1.0 (2019-10-16)
--------------------

* Move ``normalized_msgpack()`` from ``tdclient.api`` to ``tdclient.util`` module (#79)
* Add ``tdclient.util.create_msgpack()`` to support creating msgpack streaming from list (#79)


v1.0.1 (2019-10-10)
--------------------

* Fix ``wait_interval`` handling for ``BulkImport.perform`` appropriately (#74)
* Use ``io.TextIOWrapper`` to prevent ``"x85"`` issue creating None (#77)

v1.0.0 (2019-09-27)
--------------------


* Drop Python 2 support (#60)
* Remove deprecated functions as follows (#76):

  * ``TableAPI.create_item_table``
  * ``UserAPI.change_email``, ``UserAPI.change_password``, and ``UserAPI.change_my_password``
  * ``JobAPI.hive_query``, and ``JobAPI.pig_query``
* Support ``TableAPI.tail`` and ``TableAPI.change_database`` (#64, #71)
* Introduce documentation site (#65, #66, #70, #72)

v0.14.0 (2019-07-11)
--------------------


* Remove ACL and account APIs (#56, #58)
* Fix PyOpenSSL issue which causes pandas-td error (#59)

v0.13.0 (2019-03-29)
--------------------


* Change msgpack-python to msgpack (#50)
* Dropped 3.3 support as it has already been EOL'd (#52)
* Set urllib3 minimum version as v1.24.1 (#51)

v0.12.0 (2018-05-31)
--------------------


* Avoided to declare library dependencies too tightly within this project since this is a library project (#42)
* Got rid of all configurations for Python 2.6 completely (#42)

v0.11.1 (2018-05-21)
--------------------


* Added 3.6 as test target. No functional changes have applied since 0.11.0 (#41)

v0.11.0 (2018-05-21)
--------------------


* Support missing parameters in JOB API (#39, #40)

v0.10.0 (2017-11-01)
--------------------


* Ignore empty string in job's ``start_at`` and ``end_at`` (#35, #36)

v0.9.0 (2017-02-27)
-------------------


* Add validation to part names for bulk upload

v0.8.0 (2016-12-22)
-------------------


* Fix unicode encoding issues on Python 2.x (#27, #28, #29)

v0.7.0 (2016-12-06)
-------------------


* Fix for tdclient tables data not populating
* ``TableAPI.list_tables`` now returns a dictionary instead of a tuple

v0.6.0 (2016-09-27)
-------------------


* Generate universal wheel by default since there's no binary in this package
* Add missing support for ``created_time`` and ``user_name`` from ``/v3/schedule/list`` API (#20, #21)
* Use keyword arguments for initializing model attributes (#22)

v0.5.0 (2016-06-10)
-------------------


* Prevent retry after PUT request failures. This is the same behavior as https://github.com/treasure-data/td-client-ruby (#16)
* Support HTTP proxy authentication (#17)

v0.4.2 (2016-03-15)
-------------------


* Catch exceptions on parsing date time string

v0.4.1 (2016-01-19)
-------------------


* Fix Data Connector APIs based on latest td-client-ruby's implementation (#14)

v0.4.0 (2015-12-14)
-------------------


* Avoid an exception raised when a ``start`` is not set for a schedule (#12)
* Fix getting database names of job objects (#13)
* Add Data Connector APIs
* Add deprecation warnings on the usage of "item tables"
* Show ``cumul_retry_delay`` in retry messages

v0.3.2 (2015-08-01)
-------------------


* Fix bugs in ``ScheduledJob`` and ``Schedule`` models

v0.3.1 (2015-07-10)
-------------------


* Fix ``OverflowError`` on importing integer value longer than 64 bit length which is not supported by msgpack specification. Those values will be converted into string.

v0.3.0 (2015-07-03)
-------------------


* Add Python Database API (PEP 0249) compatible connection and cursor.
* Add varidation to the part name of a bulk import. It should not contain '/'.
* Changed default wait interval of job models from 1 second to 5 seconds.
* Fix many potential problems/warnings found by landscape.io.

v0.2.1 (2015-06-20)
-------------------


* Set default timeout of API client as 60 seconds.
* Change the timeout of API client from ``sum(connect_timeout, read_timeout, send_timeout)`` to ``max(connect_timeout, read_timeout, send_timeout)``
* Change default user-agent of client from ``TD-Client-Python:{version}`` to ``TD-Client-Python/{version}`` to comply RFC2616

v0.2.0 (2015-05-28)
-------------------


* Improve the job model. Now it retrieves the job values automatically after the invocation of ``wait``\ , ``result`` and ``kill``.
* Add a property ``result_schema`` to ``Job`` model to provide the schema of job result
* Improve the bulk import model. Add a convenient method named ``upload_file`` to upload a part from file-like object.
* Support CSV/TSV format on both streaming import and bulk import
* Change module name; ``tdclient.model`` -> ``tdclient.models``

v0.1.11 (2015-05-17)
--------------------


* Fix API client to retry POST requests properly if ``retry_post_requests`` is set to ``True`` (#5)
* Show warnings if imported data don't have ``time`` column

v0.1.10 (2015-03-30)
--------------------


* Fixed a JSON parse error in ``job.result_format("json")`` with multipe result rows (#4)
* Refactored model classes and tests

v0.1.9 (2015-02-26)
-------------------


* Stopped using syntax added in recent Python releases

v0.1.8 (2015-02-26)
-------------------


* Fix SSL verification errors on Python 2.7 on Windows environment.
  Now it uses ``certifi`` to verify SSL certificates if it is available.

v0.1.7 (2015-02-26)
-------------------


* Fix support for Windows environments
* Fix byte encoding problem in ``tdclient.api.API#import_file`` on Python 3.x

v0.1.6 (2015-02-12)
-------------------


* Support specifying job priority in its name (e.g. "NORMAL", "HIGH", etc.)
* Convert job priority number to its name (e.g. 0 => "NORMAL", 1 => "HIGH", etc.)
* Fix a broken behavior in ``tdclient.model.Job#wait`` when specifying timeout
* Fix broken ``tdclient.client.Client#database()`` which is used from ``tdclient.model.Table#permission()``
* Fix broken ``tdclient.Client.Client#results()``

v0.1.5 (2015-02-10)
-------------------


* Fix local variable scope problem in ``tdclient.api.show_job`` (#2)
* Fix broken multiple assignment in ``tdclient.model.Job#_update_status`` (#3)

v0.1.4 (2015-02-06)
-------------------


* Add new data import function of ``tdclient.api.import_file`` to allow importing data from
  file-like object or an existing file on filesystem.
* Fix an encoding error in ``tdclient.api.import_data`` on Python 2.x
* Add missing import to fix broken ``tdclient.model.Job#wait``
* Use ``td.api.DEFAULT_ENDPOINT`` for all requests

v0.1.3 (2015-01-24)
-------------------


* Support PEP 343 in ``tdclient.Client`` and remove ``contextlib`` from example
* Add deprecation warnings to ``hive_query`` and ``pig_query`` of ``tdclient.api.API``
* Add ``tdclient.model.Job#id`` as an alias of ``tdclient.model.Job#job_id``
* Parse datatime properly returned from ``tdclient.Client#create_schedule``
* Changed ``tdclient.model.Job#query`` as a property since it won't be modified during the execution
* Allow specifying query options from ``tdclient.model.Database#query``

v0.1.2 (2015-01-21)
-------------------


* Fix broken PyPI identifiers
* Update documentation

v0.1.1 (2015-01-21)
-------------------


* Improve the verification of SSL certificates on RedHat and variants
* Implement ``wait`` and ``kill`` in ``tdclient.model.Job``
* Change the "Development Status" from Alpha to Beta

v0.1.0 (2015-01-15)
-------------------


* Initial public release
