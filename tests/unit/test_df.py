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

import pytest
import pandas as pd
from openeew.data.df import get_df_from_records


def test_get_df_from_records_all_defaults():
    # Check that the default parameter values are used by
    # function get_df_from_records

    records = [{
        'device_id': 'test01',
        'x': [1, 2],
        'sr': 2.0,
        'cloud_t': 101.0,
        'device_t': 100.0
        }]

    expected = pd.DataFrame(
            {
                    'device_id': 'test01',
                    'x': [1, 2],
                    'sr': 2.0,
                    'cloud_t': 101.0,
                    'device_t': 100.0,
                    'sample_t': [100.5, 101.0]
                    }
            )

    pd.testing.assert_frame_equal(get_df_from_records(records), expected)


def test_get_df_from_records_all_defaults_missing_x():
    # Check that KeyError is raised when the default parameter values
    # are used by function get_df_from_records on a record without 'x' value

    records = [{
        'device_id': 'test01',
        'y': [3, 4, 5],
        'sr': 2.0,
        'cloud_t': 101.0,
        'device_t': 100.0
        }]

    with pytest.raises(KeyError, match='x'):
        # The default ref_axis value is 'x', which does not appear in
        # records, so this will raise a KeyError
        get_df_from_records(records)


def test_get_df_from_records_non_default_ref_axis():
    # Check that a non-default ref_axis parameter value is used by
    # function get_df_from_records

    records = [{
        'device_id': 'test01',
        'y': [3, 4, 5],
        'sr': 2.0,
        'cloud_t': 101.0,
        'device_t': 100.0
        }]

    expected = pd.DataFrame(
            {
                    'device_id': 'test01',
                    'y': [3, 4, 5],
                    'sr': 2.0,
                    'cloud_t': 101.0,
                    'device_t': 100.0,
                    'sample_t': [100.0, 100.5, 101.0]
                    }
            )

    pd.testing.assert_frame_equal(
            get_df_from_records(records, ref_axis='y'),
            expected
            )
