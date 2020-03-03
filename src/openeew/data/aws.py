# =============================================================================
# Copyright 2019 Grillo Holdings Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =============================================================================

import aioboto3
import boto3
import asyncio
import io
import json
from datetime import datetime, timedelta
from botocore import UNSIGNED
from botocore.client import Config


class DateTimeKeyBuilder(object):
    """
    A class for building the datetime part of keys that are
    organized by a hierarchy that can include year, month,
    day, hour and minute. The year part of the key must contain
    the century, i.e. YYYY, and other parts are
    zero-padded decimals, e.g. 01, 02 etc. An example of such
    a key template would be "year={}/month={}/day={}/hour={}/{}".
    """

    # Datetime hierarchy parts
    _DATE_PARTS = ('year', 'month', 'day', 'hour', 'minute')
    # Min possible value for each element of _DATE_PARTS
    _MIN_VALS = (1, 1, 1, 0, 0)

    def __init__(self, year, month=None, day=None,
                 hour=None, minute=None):
        """
        Initialize DateTimeKeyBuilder with the following parameters,
        which are concatenated together in order of increasing
        granularity to form key template:

        :param year: Year part of key template, e.g. "year={}/".
            Must contain {} for year replacement, where year
            will be added as YYYY, e.g. 2020.
        :type year: str

        :param month: Optional month part of key template, e.g. "month={}/".
            Must contain {} for month replacement, where month
            will be added as zero-padded number,
            i.e. 01, 02, ..., 12.
        :type month: str

        :param day: Optional day part of key template, e.g. "day={}/".
            Must contain {} for day replacement, where day
            will be added as zero-padded number,
            i.e. 01, 02, ..., 31.
        :type day: str

        :param hour: Optional hour part of key template, e.g. "hour={}/".
            Must contain {} for hour replacement, where hour
            will be added as zero-padded number,
            i.e. 00, 01, ..., 23.
        :type hour: str

        :param minute: Optional minute part of key template, e.g. "{}".
            Must contain {} for minute replacement, where minute
            will be added as zero-padded number,
            i.e. 00, 01, ..., 59.
        :type minute: str
        """
        if year is None:
            raise ValueError('year part of template must not be None')
        # Get all template parts with a value and stop as soon as we encounter
        # one without, in order of increasing granularity
        self._template_parts = []
        for p in [year, month, day, hour, minute]:
            if p is None:
                break
            self._template_parts.append(p)
        # Get the index corresponding to most granular template part
        self._idx_granularity = len(self._template_parts)-1
        # Get the granularity of our key-builder object
        self._granularity = self._DATE_PARTS[self._idx_granularity]

    @property
    def template_parts(self):
        """
        :return: A list of strings containing the defined template parts,
            in the following order: year, month, day, hour and minute. The
            length of list depends on which parts have been specified.
        :rtype: list[str]
        """
        return self._template_parts

    def _get_key_template_part(self, granularity):
        # Get the key template up to the required
        # granularity

        if granularity not in self._DATE_PARTS:
            raise ValueError(
                'Unknown granularity {}. '
                'Should be one of {}.'.format(
                    granularity,
                    self._DATE_PARTS
                    )
                )

        # If the required granularity is greater
        # than that available, use most granular available
        idx = min(
            self._DATE_PARTS.index(granularity),
            self._idx_granularity
            )

        # Simply concatenate all the parts in order
        # of granularity up to the required granularity
        return ''.join(
            self._template_parts[:idx+1]
            )

    @staticmethod
    def _get_key_from_template(key_template, dt):
        # Given a template and datetime object, dt, replace all
        # placeholders with corresponding date part values

        return key_template.format(
            '%04d' % dt.year,
            '%02d' % dt.month,
            '%02d' % dt.day,
            '%02d' % dt.hour,
            '%02d' % dt.minute
        )

    def _get_key_part(self, dt, granularity):
        # Given a datetime object, dt, return
        # the key up to the required granularity

        return self._get_key_from_template(
            self._get_key_template_part(granularity),
            dt
            )

    def get_max_key(self, dt):
        """
        Returns maximum possible key value for given datetime.

        :param dt: The datetime to build key for.
        :type dt: datetime.datetime

        :return: Key part corresponding to the datetime.
        :rtype: str
        """
        # _DATE_PARTS[-1] gives the max granularity possible
        return self._get_key_part(dt, self._DATE_PARTS[-1])

    def get_min_key(self, dt):
        """
        Returns minimum possible key value for given datetime.

        :param dt: The datetime to build key for.
        :type dt: datetime.datetime

        :return: Key part corresponding to the datetime.
        :rtype: str
        """
        # Depending on granularity of our key builder, replace
        # that part of dt with corresponding min value
        replacement = {
            self._granularity: self._MIN_VALS[self._idx_granularity]
            }

        return self._get_key_part(
            dt.replace(**replacement),
            self._granularity
            )

    def get_key_prefixes_within_range(self, start_dt, end_dt):
        """
        Returns a list of key search prefixes for the given
        date range, where each prefix corresponds to one day.

        :param start_dt: The start of the datetime range.
        :type start_dt: datetime.datetime

        :param end_dt: The end of the datetime range.
        :type end_dt: datetime.datetime

        :return: List of key search prefixes.
        :rtype: list[str]
        """
        if end_dt < start_dt:
            raise ValueError('end date should not be earlier than start date')

        key_prefixes_within_range = []
        this_dt = start_dt
        while this_dt.date() <= end_dt.date():

            this_key_part = self._get_key_part(this_dt, 'day')

            if this_key_part not in key_prefixes_within_range:
                key_prefixes_within_range.append(this_key_part)

            this_dt = this_dt + timedelta(days=1)

        return key_prefixes_within_range


class AwsDataClient(object):
    """
    A client for downloading OpenEEW data stored as an
    AWS Public Dataset.
    """

    # Template of the country part of all record keys
    _RECORDS_KEY_COUNTRY_TEMPLATE = 'records/country_code={}/'
    # Template of the device part of all record keys
    _RECORDS_KEY_DEVICE_TEMPLATE = 'device_id={}/'
    # The suffix of all record keys
    _RECORDS_KEY_SUFFIX = '.jsonl'
    # Template of the country part of all device metadata keys
    _DEVICES_KEY_TEMPLATE = 'devices/country_code={}/devices.jsonl'
    # Name of the S3 bucket where OpenEEW data is stored
    _S3_BUCKET_NAME = 'grillo-openeew'
    # Region of the S3 bucket where OpenEEW data is stored
    _S3_BUCKET_REGION = 'us-east-1'
    # Name of the time field used to assign records to files
    _RECORD_T = 'cloud_t'

    def __init__(self, country_code, s3_client=None):
        """
        Initialize AwsDataClient with the following parameters:

        :param country_code: The ISO 3166 two-letter country code
            for which data is required. It is case-insensitive
            as the value specified will be converted to lower case.
        :type country_code: str

        :param s3_client: The S3 client to use to access OpenEEW data
            on AWS. If no value is given, an anonymous S3 client
            will be used.
        :type s3_client: boto3.client.s3
        """

        self.country_code = country_code
        self._s3_client = s3_client or boto3.client(
                's3',
                region_name=self._S3_BUCKET_REGION,
                config=Config(signature_version=UNSIGNED)
                )
        # Initialize a datetime key builder for forming records keys
        self._dt_builder = DateTimeKeyBuilder(
            'year={}/', 'month={}/', 'day={}/', 'hour={}/', '{}')

    @property
    def country_code(self):
        """
        :return: ISO 3166 two-letter country code of the client
            (in lower case). Any data returned by the client will
            be for this country.
        :rtype: str
        """
        return self._country_code

    @country_code.setter
    def country_code(self, value):
        self._country_code = value.lower()
        self._records_key_country_part = \
            self._RECORDS_KEY_COUNTRY_TEMPLATE.format(self._country_code)
        self._devices_key = \
            self._DEVICES_KEY_TEMPLATE.format(self._country_code)

    @staticmethod
    def _get_dt_from_str(date_utc):
        # Parses the input string with format YYYY-mm-dd HH:MM:SS into
        # a datetime.datetime object with UTC timezone
        return datetime.strptime(
            '{} +0000'.format(date_utc),
            '%Y-%m-%d %H:%M:%S %z'
            )

    def _get_device_ids_from_records(self):
        # Returns a list of all devices (device_id) that have records

        paginator = self._s3_client.get_paginator('list_objects_v2')

        pages = paginator.paginate(
                Bucket=self._S3_BUCKET_NAME,
                Prefix=self._records_key_country_part,
                Delimiter='/'
                )

        device_prefixes = [
                p.get('Prefix') for p in pages.search('CommonPrefixes')
                ]

        # Use [:-1] to remove the final /
        return [d.split('device_id=')[1][:-1] for d in device_prefixes]

    def _get_records_key_device_prefix(self, device_id):
        # Returns a key prefix including country_code and device_id

        return self._records_key_country_part + \
            self._RECORDS_KEY_DEVICE_TEMPLATE.format(device_id)

    def _get_records_keys_to_download(self, start_dt, end_dt, device_ids=None):
        # Returns list of keys that contain required data.

        if end_dt < start_dt:
            raise ValueError('end date should not be earlier than start date')

        if device_ids is None:
            # Get list of all available devices
            _device_ids = self._get_device_ids_from_records()
        elif isinstance(device_ids, str):
            # Add single device to list
            _device_ids = [device_ids]
        elif isinstance(device_ids, list):
            # Use list directly
            _device_ids = device_ids
        else:
            raise ValueError('device_ids, if specified, should be either '
                             'a string or a list')

        # A simple lower bound for keys to download (without making
        # assumptions on num minutes contained per file) is given by
        # the hour corresponding to the start date
        start_key_date_part_min = \
            self._dt_builder.get_min_key(start_dt)

        # Get max possible key values that contain data for
        # start and end dates, respectively
        start_key_date_part_max = \
            self._dt_builder.get_max_key(start_dt)
        end_key_date_part_max = \
            self._dt_builder.get_max_key(end_dt)

        # Get list of date parts to be used as search prefixes
        key_date_prefixes_within_range = \
            self._dt_builder.get_key_prefixes_within_range(start_dt, end_dt)

        # Initialize empty list to store the keys to download
        keys_to_download = []
        paginator = self._s3_client.get_paginator('list_objects_v2')

        for d in _device_ids:

            device_prefix = self._get_records_key_device_prefix(d)

            for key_date_prefix in key_date_prefixes_within_range:
                # Initialize list to store all keys that might contain
                # data for the current device and chosen dates
                candidate_keys = []

                pages = paginator.paginate(
                            Bucket=self._S3_BUCKET_NAME,
                            Prefix=device_prefix + key_date_prefix
                            )

                for p in pages:
                    if 'Contents' not in p.keys():
                        continue
                    # Select those keys that contain data before the
                    # end date and no earlier than the hour of the
                    # start date
                    candidate_keys += [
                            o['Key'] for o in p['Contents']
                            if o['Key'] <= device_prefix +
                            end_key_date_part_max + self._RECORDS_KEY_SUFFIX
                            and o['Key'] >= device_prefix +
                            start_key_date_part_min
                            ]

                if not candidate_keys:
                    continue

                # Check which keys contain data before the start date
                keys_before_start_date = [
                        k for k in candidate_keys
                        if k <= device_prefix + start_key_date_part_max +
                        self._RECORDS_KEY_SUFFIX
                        ]

                # If no keys contain data before start date, then no
                # further filter is required and we keep all of them
                if not keys_before_start_date:
                    keys_to_download += candidate_keys
                else:
                    # Of all keys that contain data before start date,
                    # only the max of these might actually contain relevant
                    # data
                    max_key_before_start_date = max(keys_before_start_date)
                    keys_to_download += [
                            k for k in candidate_keys
                            if k >= max_key_before_start_date
                            ]

        return keys_to_download

    @classmethod
    async def _get_records_from_key(cls, async_s3_client, key):
        # Gets records from a single key and converts them to a list of dicts

        with io.BytesIO() as bytes_stream:

            await async_s3_client.download_fileobj(
                cls._S3_BUCKET_NAME,
                key,
                bytes_stream
                )

            bytes_stream.seek(0)
            records = [json.loads(line) for line in bytes_stream.readlines()]

        return records

    async def _download_keys(self, keys_to_download):
        # Gets all records from list of keys.
        # Returns a list of lists of dicts

        # Set up async S3 client using same region and config as s3_client
        async with aioboto3.client(
                's3',
                region_name=self._s3_client.meta.region_name,
                config=self._s3_client.meta.config
                ) as async_s3_client:

            # Define coroutines, one for each file to download
            coros = [
                    self._get_records_from_key(async_s3_client, k)
                    for k in keys_to_download
                        ]

            key_records = await asyncio.gather(*coros)

        return key_records

    def get_filtered_records(self, start_date_utc, end_date_utc,
                             device_ids=None):
        """
        Returns accelerometer records filtered by date and device.

        :param start_date_utc: The UTC start date
            with format %Y-%m-%d %H:%M:%S. E.g. '2018-02-16 23:39:38'.
            Only records with a _RECORD_T equal to or greater than
            start_date_utc will be returned.
        :type start_date_utc: str

        :param end_date_utc: The UTC end date with same format as
            start_date_utc. Only records with a _RECORD_T
            equal to or less than end_date_utc will be returned.
        :type end_date_utc: str

        :param device_ids: Device IDs that should be returned.
        :type device_ids: Union[str, list[str]]

        :return: A list of records, where each record is a dict
            obtained from the stored JSON value. For details about the
            JSON records, see
            `Data fields <https://github.com/grillo/openeew/tree/master/data#data-fields>`_
            for further information about how records are stored.
        :rtype: list[dict]
        """

        start_dt = self._get_dt_from_str(start_date_utc)
        end_dt = self._get_dt_from_str(end_date_utc)
        # Get the list of keys based on start and end dates

        keys_to_download = self._get_records_keys_to_download(
                start_dt,
                end_dt,
                device_ids
                )

        loop = asyncio.get_event_loop()

        key_records = loop.run_until_complete(
                self._download_keys(keys_to_download)
                )
        # Initialize empty list in which to store dicts
        records = []
        # Now loop through all keys to download and
        # concatenate the resulting lists of dicts

        for kr in key_records:
            # Keep all individual records that
            # meet the date filter
            records += [
                    d for d in kr
                    if d[self._RECORD_T] >= start_dt.timestamp() and
                    d[self._RECORD_T] <= end_dt.timestamp()
                    ]

        return records

    def get_devices_full_history(self):
        """
        Gets full history of device metadata.

        :return: A list of device metadata. See
            `Device metadata <https://github.com/grillo/openeew/tree/master/data#device-metadata>`_
            for information about the fields.
        :rtype: list[dict]
        """

        bytes_stream = io.BytesIO()
        self._s3_client.download_fileobj(
                self._S3_BUCKET_NAME,
                self._devices_key,
                bytes_stream
                )
        bytes_stream.seek(0)
        devices = [json.loads(line) for line in bytes_stream.readlines()]
        bytes_stream.close()

        return devices

    def get_current_devices(self):
        """
        Gets currently-valid device metadata. Fields are the same as
        for :func:`get_devices_full_history`.

        :return: A list of device metadata.
        :rtype: list[dict]
        """

        return [
                d for d in self.get_devices_full_history()
                if d['is_current_row']
                ]

    def get_devices_as_of_date(self, date_utc):
        """
        Gets device metadata as of a chosen UTC date. Fields are the
        same as for :func:`get_devices_full_history`.

        :param date_utc: The UTC date with format %Y-%m-%d %H:%M:%S.
            E.g. '2018-02-16 23:39:38'.
        :type date_utc: str

        :return: A list of device metadata.
        :rtype: list[dict]
        """

        ts = self._get_dt_from_str(date_utc).timestamp()

        return [
                d for d in self.get_devices_full_history()
                if d['effective_from'] <= ts and
                d['effective_to'] >= ts
                ]
