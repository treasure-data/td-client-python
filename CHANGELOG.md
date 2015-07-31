# Version History

## 0.3.2.dev0 (2015-07-31)

* Fix bugs in `ScheduledJob` and `Schedule` models

## 0.3.1 (2015-07-10)

* Fix `OverflowError` on importing integer value longer than 64 bit length which is not supported by msgpack specification. Those values will be converted into string.

## 0.3.0 (2015-07-03)

* Add Python Database API (PEP 0249) compatible connection and cursor.
* Add varidation to the part name of a bulk import. It should not contain '/'.
* Changed default wait interval of job models from 1 second to 5 seconds.
* Fix many potential problems/warnings found by landscape.io.

## 0.2.1 (2015-06-20)

* Set default timeout of API client as 60 seconds.
* Change the timeout of API client from `sum(connect_timeout, read_timeout, send_timeout)` to `max(connect_timeout, read_timeout, send_timeout)`
* Change default user-agent of client from `TD-Client-Python:{version}` to `TD-Client-Python/{version}` to comply RFC2616

## 0.2.0 (2015-05-28)

* Improve the job model. Now it retrieves the job values automatically after the invocation of `wait`, `result` and `kill`.
* Add a property `result_schema` to `Job` model to provide the schema of job result
* Improve the bulk import model. Add a convenient method named `upload_file` to upload a part from file-like object.
* Support CSV/TSV format on both streaming import and bulk import
* Change module name; `tdclient.model` -> `tdclient.models`

## 0.1.11 (2015-05-17)

* Fix API client to retry POST requests properly if `retry_post_requests` is set to `True` (#5)
* Show warnings if imported data don't have `time` column

## 0.1.10 (2015-03-30)

* Fixed a JSON parse error in `job.result_format("json")` with multipe result rows (#4)
* Refactored model classes and tests

## 0.1.9 (2015-02-26)

* Stopped using syntax added in recent Python releases

## 0.1.8 (2015-02-26)

* Fix SSL verification errors on Python 2.7 on Windows environment.
  Now it uses `certifi` to verify SSL certificates if it is available.

## 0.1.7 (2015-02-26)

* Fix support for Windows environments
* Fix byte encoding problem in `tdclient.api.API#import_file` on Python 3.x

## 0.1.6 (2015-02-12)

* Support specifying job priority in its name (e.g. "NORMAL", "HIGH", etc.)
* Convert job priority number to its name (e.g. 0 => "NORMAL", 1 => "HIGH", etc.)
* Fix a broken behavior in `tdclient.model.Job#wait` when specifying timeout
* Fix broken `tdclient.client.Client#database()` which is used from `tdclient.model.Table#permission()`
* Fix broken `tdclient.Client.Client#results()`

## 0.1.5 (2015-02-10)

* Fix local variable scope problem in `tdclient.api.show_job` (#2)
* Fix broken multiple assignment in `tdclient.model.Job#_update_status` (#3)

## 0.1.4 (2015-02-06)

* Add new data import function of `tdclient.api.import_file` to allow importing data from
  file-like object or an existing file on filesystem.
* Fix an encoding error in `tdclient.api.import_data` on Python 2.x
* Add missing import to fix broken `tdclient.model.Job#wait`
* Use `td.api.DEFAULT_ENDPOINT` for all requests

## 0.1.3 (2015-01-24)

* Support PEP 343 in `tdclient.Client` and remove `contextlib` from example
* Add deprecation warnings to `hive_query` and `pig_query` of `tdclient.api.API`
* Add `tdclient.model.Job#id` as an alias of `tdclient.model.Job#job_id`
* Parse datatime properly returned from `tdclient.Client#create_schedule`
* Changed `tdclient.model.Job#query` as a property since it won't be modified during the execution
* Allow specifying query options from `tdclient.model.Database#query`

## 0.1.2 (2015-01-21)

* Fix broken PyPI identifiers
* Update documentation

## 0.1.1 (2015-01-21)

* Improve the verification of SSL certificates on RedHat and variants
* Implement `wait` and `kill` in `tdclient.model.Job`
* Change the "Development Status" from Alpha to Beta

## 0.1.0 (2015-01-15)

* Initial public release
