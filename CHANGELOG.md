# Version History

## 0.2.1

* Set default timeout of API client as 60 seconds.
* Change the timeout of API client from `sum(connect_timeout, read_timeout, send_timeout)` to `max(connect_timeout, read_timeout, send_timeout)`
* Change default user-agent of client from `TD-Client-Python:{version}` to `TD-Client-Python/{version}` to comply RFC2616

## 0.2.0

* Improve the job model. Now it retrieves the job values automatically after the invocation of `wait`, `result` and `kill`.
* Add a property `result_schema` to `Job` model to provide the schema of job result
* Improve the bulk import model. Add a convenient method named `upload_file` to upload a part from file-like object.
* Support CSV/TSV format on both streaming import and bulk import
* Change module name; `tdclient.model` -> `tdclient.models`

## 0.1.11

* Fix API client to retry POST requests properly if `retry_post_requests` is set to `True` (#5)
* Show warnings if imported data don't have `time` column

## 0.1.10

* Fixed a JSON parse error in `job.result_format("json")` with multipe result rows (#4)
* Refactored model classes and tests

## 0.1.9

* Stopped using syntax added in recent Python releases

## 0.1.8

* Fix SSL verification errors on Python 2.7 on Windows environment.
  Now it uses `certifi` to verify SSL certificates if it is available.

## 0.1.7

* Fix support for Windows environments
* Fix byte encoding problem in `tdclient.api.API#import_file` on Python 3.x

## 0.1.6

* Support specifying job priority in its name (e.g. "NORMAL", "HIGH", etc.)
* Convert job priority number to its name (e.g. 0 => "NORMAL", 1 => "HIGH", etc.)
* Fix a broken behavior in `tdclient.model.Job#wait` when specifying timeout
* Fix broken `tdclient.client.Client#database()` which is used from `tdclient.model.Table#permission()`
* Fix broken `tdclient.Client.Client#results()`

## 0.1.5

* Fix local variable scope problem in `tdclient.api.show_job` (#2)
* Fix broken multiple assignment in `tdclient.model.Job#_update_status` (#3)

## 0.1.4

* Add new data import function of `tdclient.api.import_file` to allow importing data from
  file-like object or an existing file on filesystem.
* Fix an encoding error in `tdclient.api.import_data` on Python 2.x
* Add missing import to fix broken `tdclient.model.Job#wait`
* Use `td.api.DEFAULT_ENDPOINT` for all requests

## 0.1.3

* Support PEP 343 in `tdclient.Client` and remove `contextlib` from example
* Add deprecation warnings to `hive_query` and `pig_query` of `tdclient.api.API`
* Add `tdclient.model.Job#id` as an alias of `tdclient.model.Job#job_id`
* Parse datatime properly returned from `tdclient.Client#create_schedule`
* Changed `tdclient.model.Job#query` as a property since it won't be modified during the execution
* Allow specifying query options from `tdclient.model.Database#query`

## 0.1.2

* Fix broken PyPI identifiers
* Update documentation

## 0.1.1

* Improve the verification of SSL certificates on RedHat and variants
* Implement `wait` and `kill` in `tdclient.model.Job`
* Change the "Development Status" from Alpha to Beta

## 0.1.0

* Initial public release
