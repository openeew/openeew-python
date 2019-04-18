====
OpenEEW library for Python
====

The OpenEEW library for Python (versions 3.4 and above) will contain a collection of tools for working with OpenEEW, including:

* downloading and analyzing accelerometer data
* sensor firmware
* detection algorithms

Currently it provides a client to simplify downloading data held as an `AWS Public Dataset <https://registry.opendata.aws/grillo-openeew/>`_. See `here <https://github.com/grillo/openeew/tree/master/data#accessing-openeew-data-on-aws>`_ for information about how data is organized.

More library features will be coming soon.

Installation
===========

The library can be installed using ``pip`` (it should correspond to Python 3.4 or above)::

  pip install openeew

Usage
===========
Here is an example of how the library can be used to download data for a chosen date range.

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

  records_df = data_client.get_filtered_records_df(
    start_date_utc,
    end_date_utc
    )
    
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
This library is distrubted under the Apache License, Version 2.0. See `LICENSE <LICENSE>`_.
