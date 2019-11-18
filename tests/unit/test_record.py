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
from openeew.data.record import add_sample_t, add_sample_t_to_records

# A record with two axes, x and y, each having different length.
# This is to test that the length of the specified ref_axis is used
# to calculate sample_t. Note that we do not expect records to have
# axes with different lengths in practice, it is used here just
# to make sure the ref_axis parameter is being used.
record_axes_different_lengths = {
        'x': [1, 2],
        'y': [3, 4, 5],
        'sr': 2.0,
        't': 100.0
        }

# This is a list of tuples (record, ref_axis, expected).
test_record_axes_different_lengths = [
        # First record: Use y-axis; sample_t has length 3
        (
                record_axes_different_lengths,
                'y',
                {
                        **record_axes_different_lengths,
                        'sample_t': [99.0, 99.5, 100.0]
                        }
                ),
        # Second record: Use x-axis; sample_t has length 2
        (
                record_axes_different_lengths,
                'x',
                {
                        **record_axes_different_lengths,
                        'sample_t': [99.5, 100.0]
                        }
                )
        ]


@pytest.mark.parametrize(
        'record,ref_axis,expected',
        test_record_axes_different_lengths
        )
def test_add_sample_t_ref_axis(record, ref_axis, expected):

    assert add_sample_t(record, 't', ref_axis) == expected


@pytest.mark.parametrize(
        'record,ref_axis,expected',
        test_record_axes_different_lengths
        )
def test_add_sample_t_to_records_ref_axis(record, ref_axis, expected):
    # Here we just add the input and expected records to a list
    assert add_sample_t_to_records([record], 't', ref_axis) == [expected]
