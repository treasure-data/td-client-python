File import parameters
======================

``str or file-like`` parameters specify where to read the input data
from. They can be:

* a file name.
* a file object, representing a file opened in binary mode.
* an object that acts like an instance of `io.BufferedIOBase`_. Reading from it
  returns bytes.

``format`` is a string specifying an input format.
The following input formats are supported:

* "msgpack" - the data is MessagePack_ serialized
* "json" - the data is JSON_ serialized.
* "csv" - the data is CSV, and will be read using the `Python CSV module`_.
* "tsv" - the data is TSV (tab separated data), and will be read using the
  `Python CSV module`_ with ``dialect=csv.excel_tab`` explicitly set.

.. _`io.BufferedIOBase`: https://docs.python.org/3/library/io.html#io.BufferedIOBase
.. _MessagePack: https://msgpack.org/
.. _JSON: https://www.json.org/
.. _`Python CSV module`: https://docs.python.org/3/library/csv.html

If ``.gz`` is appended to the format name (for instance, ``"json.gz"``) then
the data is assumed to be gzip compressed, and will be uncompressed as it is
read.

Both MessagePack and JSON data are composed of an array of records, where each
record is a dictionary (hash or mapping) of column name to column value.

In all import formats, every record must have a column named "time".

JSON data
---------

JSON data is read using the utf-8 encoding.

CSV data
--------

When reading CSV data, the following parameters may also be supplied,
all of which are optional:

* ``dialect`` specifies the CSV dialect. The default is ``csv.excel``.
* ``encoding`` specifies the encoding that will be used to turn the binary
  input data into string data. The default encoding is ``"utf-8"``
* ``columns`` is a list of strings, giving names for the CSV
  columns. The default is ``None``, meaning that the column names will be
  taken from the first record in the CSV data.
* ``dtypes`` is a dictionary used to specify a datatype for individual
  columns, for instance ``{"col1": "int"}``. The available datatypes are
  ``"bool"``, ``"float"``, ``"int"``, ``"str"`` and ``"guess"``, where
  ``"guess"`` means to use the function guess_csv_value_.
* ``converters`` is a dictionary used to specify a function that will be used
  to parse individual columns, for instace ``{"col1", int}``. The function
  must take a string as its single input parameter, and return a value of the
  required type.

If a column is named in both ``dtypes`` and ``converters``, then the function
given in ``converters`` will be used to parse that column.

If a column is not named in either ``dtypes`` or ``converters``, then it will
be assumed to have datatype ``"guess"``, and will be parsed with
guess_csv_value_.

Note that errors raised when calling a function from the ``converters``
dictionary will not be caught. So if ``converters={"col1": int}`` and "col1"
contains ``"not-an-int"``, the resulting ``ValueError`` will not be caught.

.. _guess_csv_value: api/misc.html#tdclient.util.guess_csv_value

To summarise, the default for reading CSV files is:

  ``dialect=csv.excel, encoding="utf-8", columns=None, dtypes=None, converters=None``
  
TSV data
--------

When reading TSV data, the parameters that may be used are the same as for
CSV, except that:

* ``dialect`` may not be specified, and ``csv.excel_tab`` will be used.

The default for reading TSV files is:

  ``encoding="utf-8", columns=None, dtypes=None, converters=None``
