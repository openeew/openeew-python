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

from openeew.data.record import add_sample_t_to_records
import pandas as pd


def get_df_from_records(records, ref_t_name='cloud_t'):
    """
    Returns a pandas DataFrame from a list of records.

    :param records: The list of records from which
        to create a pandas DataFrame.
    :type records: list[dict]

    :param ref_t_name: The name of the time field to use as a reference when
        calculating sample times. This should be either cloud_t or device_t.
    :type ref_t_name: str

    :return: A pandas DataFrame with columns the same as
        the keys of each record and an additional sample_t column
        giving an individual timestamp to each of the x, y and z
        array elements.
    :rtype: pandas.DataFrame
    """
    # Make sure records list is not empty before proceeding
    if not records:
        raise ValueError('The list of records should be non-empty')

    # Add list of sample times to each record
    records = add_sample_t_to_records(records, ref_t_name)
    # Concatenate all dicts into a single DataFrame
    records_df = pd.concat(
            [pd.DataFrame.from_dict(r) for r in records]
            )
    # Sort values by device and then in chronological order
    records_df = records_df.sort_values(
            ['device_id', 'sample_t', 'device_t']
            )

    return records_df
