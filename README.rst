====
OpenEEW package for Python
====

.. image:: https://img.shields.io/pypi/v/openeew.svg
    :target: https://pypi.python.org/pypi/openeew/

.. image:: https://img.shields.io/pypi/pyversions/openeew.svg
    :target: https://pypi.python.org/pypi/openeew/

.. image:: https://travis-ci.org/grillo/openeew-python.svg?branch=master
    :target: https://travis-ci.org/grillo/openeew-python
   
.. image:: https://readthedocs.org/projects/openeew-python/badge/?version=latest
    :target: https://openeew-python.readthedocs.io/en/latest/?badge=latest

The OpenEEW package for Python will contain a collection of tools for working with OpenEEW, including:

* downloading and analyzing accelerometer data
* real-time data processing
* detection algorithms

Currently it provides a client to simplify downloading data held as an `AWS Public Dataset <https://registry.opendata.aws/grillo-openeew/>`_. See `here <https://github.com/grillo/openeew/tree/master/data#accessing-openeew-data-on-aws>`_ for information about how data is organized.

More features will be coming soon.

Some additional resources:

* Package `documentation <https://openeew-python.readthedocs.io>`_
* `OpenEEW <https://github.com/grillo/openeew>`_ GitHub repository

Installation
===========

The package can be installed using ``pip`` (for a suitable version of Python)::

  pip install openeew

Usage
===========
Here is an example of how the package can be used to download data for a chosen date range.

First import the ``AwsDataClient`` class::

  from openeew.data.aws import AwsDataClient
  
We initialize a data client object by specifying the required country, either ``mx`` for Mexico or ``cl`` for Chile::

  data_client = AwsDataClient('mx')

To get a list of current devices::

  devices = data_client.get_current_devices()
  
To get data for a specific (UTC) date range, first specify the start and end dates::

  start_date_utc = '2018-02-16 23:39:00'
  end_date_utc = '2018-02-16 23:42:00'
  
Then call the ``get_filtered_records`` method::

  records = data_client.get_filtered_records(
    start_date_utc,
    end_date_utc
    )
    
We can also get the data as a pandas.DataFrame::

  from openeew.data.df import get_df_from_records

  records_df = get_df_from_records(records)
    
It is also possible to specify a list of device IDs::

  records = data_client.get_filtered_records(
    start_date_utc,
    end_date_utc,
    ['001', '008'] # optional list of device IDs
    )
    
We can change the country if we want. So if we now want to look at Chile data::

  data_client.country_code = 'cl'

Contributing
===========
This project welcomes contributions. See `CONTRIBUTING.rst <CONTRIBUTING.rst>`_ for more information.

License
===========
This package is distrubted under the Apache License, Version 2.0. See `LICENSE <LICENSE>`_.
