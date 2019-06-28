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


def get_sample_t(ref_t, idx, num_samples, sr):
    """
    Calculates Unix time for individual sample point (within a record).
    The sample point time is calculated by subtracting a multiple of
    1/sr from the reference time corresponding to the final sample point
    in the record, where sr is the sample rate.

    :param ref_t: The Unix time to use as a basis for the calculation.
    :type ref_t: float

    :param idx: The zero-based index of the sample point within the record.
    :type idx: int

    :param num_samples: The number of sample points within the record.
    :type num_samples: int

    :param sr: The sample rate (per second) of the record.
    :type sr: float

    :return: An estimated Unix time corresponding to the sample point.
    :rtype: float
    """

    return ref_t - ((1/sr)*(num_samples - 1 - idx))


def add_sample_t(record, ref_t_name):
    """
    Adds a list of sample times to a record corresponding to
    each sample point in the record.

    :param record: The record to which to add sample times.
    :type record: dict

    :param ref_t_name: The name of the time field to use as a reference when
        calculating sample times. This should be either cloud_t or device_t.
    :type ref_t_name: str

    :return: A record with additional sample_t field containing
        list of sample times.
    :rtype: dict
    """

    return {
            **record,
            'sample_t': [
                    round(get_sample_t(
                                record[ref_t_name],
                                i,
                                len(record['x']),
                                record['sr']
                                ), 3)
                    for i in range(0, len(record['x']))
                    ]
            }


def add_sample_t_to_records(records, ref_t_name):
    """
    Adds sample_t field to each record in a list of records.

    :param records: The list of records to which to add sample times.
    :type records: list[dict]

    :param ref_t_name: The name of the time field to use as a reference when
        calculating sample times. This should be either cloud_t or device_t.
    :type ref_t_name: str

    :return: A list of records with additional sample_t field containing
        list of sample times.
    :rtype: list[dict]
    """

    return [add_sample_t(r, ref_t_name) for r in records]
