import unittest2 as unittest
import clean_data


class TestCleanData(unittest.TestCase):

    # Test for get_affected_indexes function
    def test_get_affected_indexes(self):
        result = clean_data.get_state_indexes(['A', 'A', 'STATE', 'A', 'STATE'])

        self.assertEqual(result, [2, 4])

    # Tests for get_affected_message_indexes_per_STATE function
    # CASE 1: When there is only one message affected corresponding to the STATE index
    def test_affected_indexes_case_1(self):
        result = clean_data.get_affected_message_indexes_per_STATE(2, ['1', '1', '1', '1', '1'],
                                                                   ['A', 'A', 'STATE', 'A', 'A'],
                                                                   ['1', '2', '12', '18', '60'])
        self.assertEqual(result, [2])

        # CASE 2: When there is more than a message affected (Including STATE index) but only backward

    def test_affected_indexes_case_2(self):
        result = clean_data.get_affected_message_indexes_per_STATE(2, ['1', '1', '1', '1', '1'],
                                                                   ['A', 'A', 'STATE', 'W', 'A'],
                                                                   ['1', '2', '3', '9', '60'])
        self.assertEqual(result, [0, 1, 2])

        # CASE 3: When there is more than a message affected (Including STATE index) but only forward

    def test_affected_indexes_case_3(self):
        result = clean_data.get_affected_message_indexes_per_STATE(2, ['1', '1', '1', '1', '1'],
                                                                   ['A', 'A', 'STATE', 'W', 'A'],
                                                                   ['1', '2', '12', '14', '17'])
        self.assertEqual(result, [2, 3, 4])


if __name__ == '__main__':
    unittest.main()
