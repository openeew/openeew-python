====
OpenEEW library for Python
====

The OpenEEW library for Python (versions 3.4 and above) will contain a collection of tools for working with OpenEEW, including:

* downloading and analyzing accelerometer data
* sensor firmware
* detection algorithms

Currently it provides a client to simplify downloading data held as an `AWS Public Dataset <https://registry.opendata.aws/grillo-openeew/>`_. More features will be coming soon.

Installation
===========

The library can be installed using ``pip`` (it should correspond to Python 3.4 or above)::

  pip install openeew

Usage
===========
Here is an example of how the library can be used to download data for a chosen date range::

  from openeew.data.aws import AwsDataClient
  
  # Initialize a data client, specifying the required country
  # Options are 'mx' for Mexico and 'cl' for Chile
  data_client = AwsDataClient('mx')
  
  # Get a list of current devices
  devices = data_client.get_current_devices()
  
  # Choose UTC date range of interest:
  start_date_utc = '2018-02-16 23:39:00'
  end_date_utc = '2018-02-16 23:42:00'
  
  # Get data from all devices for
  # the chosen UTC date range
  records = data_client.get_filtered_records(
    start_date_utc,
    end_date_utc
    )
    
  # We can also get the data as a pandas.DataFrame
  records_df = data_client.get_filtered_records_df(
    start_date_utc,
    end_date_utc
    )
    
  # It is also possible to specify a list of device IDs
  records = data_client.get_filtered_records(
    start_date_utc,
    end_date_utc,
    ['001', '008'] # optional list of device IDs
    )
    
  # Now we can change the country if we want
  # to download Chile data
  data_client.country_code = 'cl'

Contributing
===========
This project welcomes contributions. See `CONTRIBUTING.md <CONTRIBUTING.md>`_ for more information.

License
===========
This library is distrubted under the Apache License, Version 2.0. See `LICENSE <LICENSE>`_.
