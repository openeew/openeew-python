=========
Changelog
=========

openeew uses `Semantic Versioning <http://semver.org/>`_

Unreleased
=============
- Allow to get number of sample points in a record using general axis labels
- Pass single device ID as string when retrieving records
- Filter records based on time field name in class attribute

Version 0.5.0
=============
- Use one async client to download all files.

Version 0.4.0
=============
- Use asyncio to speed up downloading of files in AwsDataClient
- Speed up search for S3 keys within specified date range
- Updated some AwsDataClient method decorators

Version 0.3.0
=============
- Added df submodule to openeew.data.
  The method get_filtered_records_df of AwsDataClient in openeew.data.aws
  has been removed and instead the function get_df_from_records in
  openeew.data.df can be used to return a pandas DataFrame
  from a list of records.
- Fixed/updated docstrings

Version 0.2.0
=============
- Added record submodule to openeew.data

Version 0.1.3
=============
- Fixed/updated docstrings
- Removed Python 3.4 from requirements

Version 0.1.2
=============

- Initial version released on PyPI.
