import unittest2 as unittest
import more_specifics_analysis
import pandas as pd


class TestMoreSpecificsAnalysis(unittest.TestCase):

    def test_get_withdraw_indexes(self):

        result = more_specifics_analysis.get_withdraw_indexes(pd.DataFrame(
            {'TIME': ['1', '2', '3', '3', '4', '5'], 'TYPE': ['W', 'A', 'W', 'A', 'W', 'W'],
             'MONITOR': ['192.168.0.0', '192.168.0.0', '192.168.0.0', '192.168.0.0', '192.168.0.0', '192.168.0.0'],
             'AS': ['123', '6001', '342', '1231', '3254', '43265'],
             'PREFIX': ['193.169.130.0/23', '118.184.200.0/21', '118.184.224.0/21', '118.184.232.0/21',
                        '78.130.255.0/24', '213.145.111.0/24'],
             'AS_PATH': [['45896', '6939', '15412', '29357', '43852', '43852', '43852', '43852', '43852', '21050'],
                         ['45896', '3356', '174', '20473', '18257'], ['45896', '3356', '174', '20473', '18257'],
                         ['45896', '3356', '174', '20473', '18257'], ['45896', '3356', '60262', '207214'],
                         ['45896', '3356', '60262', '207214']]}))

        self.assertEqual(result, [0, 2, 4, 5])


if __name__ == '__main__':
    unittest.main()
