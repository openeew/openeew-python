=========
Changelog
=========

openeew uses `Semantic Versioning <http://semver.org/>`_

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
