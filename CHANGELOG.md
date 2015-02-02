# Version History

## 0.1.4.dev0

* Add new data import function of `tdclient.api.import_file` to allow importing data from
  file-like object or an existing file on filesystem.
* Fix an encoding error in `tdclient.api.import_data` on Python 2.x

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
