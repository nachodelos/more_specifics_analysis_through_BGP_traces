import unittest2 as unittest
import more_specifics_analysis
import pandas as pd


class TestMoreSpecificsAnalysis(unittest.TestCase):

    def test_get_change_index_per_column_case1(self):
        result = more_specifics_analysis.get_change_index_per_column([1, 2, 2, 3, 4, 4, 4, 5, 5], 0, 8)
        self.assertEqual(result, [0, 1, 3, 4, 7])

    def test_get_change_index_per_column_case2(self):
        result = more_specifics_analysis.get_change_index_per_column([1, 2, 2, 3, 4, 4, 4, 5, 6], 0, 8)
        self.assertEqual(result, [0, 1, 3, 4, 7])

    def test_get_accum_time_per_prefix_case_1(self):
        df_sort = pd.DataFrame({'TIMES': [1, 2, 5, 6, 7], 'TYPES': ['B', 'A', 'A', 'A', 'A']})
        result = more_specifics_analysis.get_visibility_time_per_prefix(df_sort['TIMES'][0:3], df_sort['TYPES'][0:3],
                                                                        float(1), float(8), range(0, 3))
        expected_result = 1
        self.assertEqual(result, expected_result)

    def test_get_accum_time_per_prefix_case_2(self):
        df_sort = pd.DataFrame({'TIMES': [1, 2, 5, 6, 7], 'TYPES': ['B', 'A', 'A', 'A', 'A']})
        result = more_specifics_analysis.get_visibility_time_per_prefix(df_sort['TIMES'][3:5], df_sort['TYPES'][3:5],
                                                                        float(1), float(8), range(3, 5))
        expected_result = (8 - 6) / (float(8) - float(1))
        self.assertEqual(result, expected_result)

    def test_get_accum_time_per_prefix_case_3(self):
        df_sort = pd.DataFrame({'TIMES': [2], 'TYPES': ['A']})
        result = more_specifics_analysis.get_visibility_time_per_prefix(df_sort['TIMES'][0:1], df_sort['TYPES'][0:1],
                                                                        float(1), float(8), range(0, 1))
        expected_result = (8 - 2) / (float(8) - float(1))
        self.assertEqual(result, expected_result)

    def test_get_accum_time_per_prefix_case_4(self):
        df_sort = pd.DataFrame({'TIMES': [1], 'TYPES': ['B']})
        result = more_specifics_analysis.get_visibility_time_per_prefix(df_sort['TIMES'][0:1], df_sort['TYPES'][0:1],
                                                                        float(1), float(8), range(0, 1))
        expected_result = 1
        self.assertEqual(result, expected_result)

    def test_get_accum_time_per_prefix_case_5(self):
        df_sort = pd.DataFrame({'TIMES': [2], 'TYPES': ['A']})
        result = more_specifics_analysis.get_visibility_time_per_prefix(df_sort['TIMES'][0:1], df_sort['TYPES'][0:1],
                                                                        float(1), float(8), range(0, 1))
        expected_result = (8 - 2) / (float(8) - float(1))
        self.assertEqual(result, expected_result)

    def test_get_accum_time_per_prefix_case_6(self):
        df_sort = pd.DataFrame({'TIMES': [1, 2, 5, 6, 7], 'TYPES': ['B', 'A', 'A', 'A', 'A']})
        result = more_specifics_analysis.get_visibility_time_per_prefix(df_sort['TIMES'][0:5], df_sort['TYPES'][0:5],
                                                                        float(1), float(8), range(0, 5))
        expected_result = 1
        self.assertEqual(result, expected_result)

    def test_get_accum_time_per_prefix_case_7(self):
        df_sort = pd.DataFrame({'TIMES': [1, 2, 5, 6, 7], 'TYPES': ['B', 'A', 'W', 'A', 'A']})
        result = more_specifics_analysis.get_visibility_time_per_prefix(df_sort['TIMES'][0:5], df_sort['TYPES'][0:5],
                                                                        float(1), float(8), range(0, 5))
        expected_result = ((8 - 6) + (5 - 1)) / (float(8) - float(1))
        self.assertEqual(result, expected_result)

    def test_get_accum_time_per_prefix_case_8_negative_results(self):
        df_sort = pd.DataFrame({'TIMES': [1515369600, 1515386492, 1515386493, 1515386493, 1515386494, 1515386494],
                                'TYPES': ['B', 'A', 'A', 'W', 'A', 'A']})
        result = more_specifics_analysis.get_visibility_time_per_prefix(df_sort['TIMES'][0:6], df_sort['TYPES'][0:6],
                                                                        float(1515369600), float(1515398400),
                                                                        range(0, 6))
        expected_result = ((1515386493 - 1515369600) + (1515398400 - 1515386494)) / (
                    float(1515398400) - float(1515369600))
        self.assertEqual(expected_result, result)

    def test_get_accum_time_per_prefix_case_8_negative_results_2(self):
        df_sort = pd.DataFrame({'TIMES': [1515398459, 1515398460, 1515398466, 1515398468, 1515398472],
                                'TYPES': ['A', 'A', 'A', 'A', 'A']})
        result = more_specifics_analysis.get_visibility_time_per_prefix(df_sort['TIMES'][0:5], df_sort['TYPES'][0:5],
                                                                        float(1515369600), float(1515398400),
                                                                        range(0, 5))
        expected_result = ((1515398460 - 1515398459) + (1515398466 - 1515398460) + (1515398468 - 1515398466)) / (float(1515398400) - float(1515369600))
        self.assertEqual(expected_result, result)

    #def test_clustering_prefixes(self):
    #	df_pref_per_monitor = pd.DataFrame({'MONITOR': [] })	

    # def test_split_data_per_monitor(self):
    #     result = more_specifics_analysis.split_data_per_monitor(pd.DataFrame(
    #         {'TIME': ['1', '2', '3', '3', '4', '5'],
    #          'TYPE': ['B', 'A', 'W', 'A', 'W', 'W'],
    #          'MONITOR': ['192.168.0.0', '192.168.0.0', '192.168.0.1', '192.168.0.0', '192.168.0.0', '192.168.0.0'],
    #          'AS': ['123', '6001', '342', '1231', '3254', '43265'],
    #          'PREFIX': ['193.169.130.0/23', '118.184.200.0/21', '118.184.224.0/21', '118.184.232.0/21',
    #                     '78.130.255.0/24', '213.145.111.0/24'],
    #          'AS_PATH': [['45896', '6939', '15412', '29357', '43852', '43852', '43852', '43852', '43852', '21050'],
    #                      ['45896', '3356', '174', '20473', '18257'], ['45896', '3356', '174', '20473', '18257'],
    #                      ['45896', '3356', '174', '20473', '18257'], ['45896', '3356', '60262', '207214'],
    #                      ['45896', '3356', '60262', '207214']]}))
    #
    #     for monitor in result:
    #         self.assertEqual(pd.DataFrame.from_dict(result['PREFIX'].all()), pd.DataFrame.from_dict({'PREFIX': ['118.184.224.0/21'], 'TIME': ['3'], 'TYPE': ['W']}) ['PREFIX'].all())
    #
    #     self.assertEqual(result,
    #                      {'192.168.0.0': pd.DataFrame.from_dict({'PREFIX': ['193.169.130.0/23', '118.184.200.0/21', '118.184.232.0/21',
    #                                                  '78.130.255.0/24', '213.145.111.0/24'],
    #                                       'TIME': ['1', '2', '3', '4', '5'], 'TYPE': ['B', 'A', 'A', 'W', 'W']}),
    #                       '192.168.0.1': pd.DataFrame.from_dict({'PREFIX': ['118.184.224.0/21'], 'TIME': ['3'], 'TYPE': ['W']})})
    #
    # def test_split_data_per_prefix(self):
    #     result = more_specifics_analysis.split_data_per_prefix(
    #         {'192.168.0.0': pd.DataFrame.from_dict({'PREFIX': ['193.169.130.0/23', '118.184.200.0/21', '118.184.200.0/21',
    #                                     '78.130.255.0/24', '213.145.111.0/24'],
    #                          'TIME': ['1', '2', '3', '4', '5'], 'TYPE': ['B', 'A', 'A', 'W', 'W']}),
    #          '192.168.0.1': pd.DataFrame.from_dict({'PREFIX': ['118.184.224.0/21'], 'TIME': ['3'], 'TYPE': ['W']})})
    #
    #     self.assertEqual(result,
    #                      {'192.168.0.0': {{'193.169.130.0/23': {'TIME': ['1'], 'TYPE': ['B']}},
    #                                       {'118.184.200.0/21': {'TIME': ['2', '3'], 'TYPE': ['A', 'A']}},
    #                                       {'78.130.255.0/24': {'TIME': ['4'], 'TYPE': ['W']}},
    #                                       {'213.145.111.0/24': {'TIME': ['5'], 'TYPE': ['W']}}, },
    #                       '192.168.0.1': {{'118.184.224.0/21': {'TIME': ['3'], 'TYPE': ['W']}}}})

    # def test_get_stability_time_per_prefix(self):
    #     result = more_specifics_analysis.get_stability_time_per_prefix(
    #         {'192.168.0.0': {'PREFIX': ['193.169.130.0/23', '118.184.200.0/21', '118.184.232.0/21',
    #                                     '78.130.255.0/24', '213.145.111.0/24'],
    #                          'TIMES': ['1', '2', '3', '4', '5'], 'TYPES': ['B', 'A', 'A', 'W', 'W']},
    #          '192.168.0.1': {'PREFIX': ['118.184.224.0/21'], 'TIMES': ['3'], 'TYPES': ['W']}}, 1, 6)
    #     print result

    # def test_get_withdraw_indexes(self):
    #     result = more_specifics_analysis.get_withdraw_indexes(pd.DataFrame(
    #         {'TIME': ['1', '2', '3', '3', '4', '5'],
    #          'TYPE': ['W', 'A', 'W', 'A', 'W', 'W'],
    #          'MONITOR': ['192.168.0.0', '192.168.0.0', '192.168.0.0', '192.168.0.0', '192.168.0.0', '192.168.0.0'],
    #          'AS': ['123', '6001', '342', '1231', '3254', '43265'],
    #          'PREFIX': ['193.169.130.0/23', '118.184.200.0/21', '118.184.224.0/21', '118.184.232.0/21',
    #                     '78.130.255.0/24', '213.145.111.0/24'],
    #          'AS_PATH': [['45896', '6939', '15412', '29357', '43852', '43852', '43852', '43852', '43852', '21050'],
    #                      ['45896', '3356', '174', '20473', '18257'], ['45896', '3356', '174', '20473', '18257'],
    #                      ['45896', '3356', '174', '20473', '18257'], ['45896', '3356', '60262', '207214'],
    #                      ['45896', '3356', '60262', '207214']]}))
    #
    #     self.assertEqual(result, [0, 2, 4, 5])
    #
    # def test_delete_withdraw_updates(self):
    #     result = more_specifics_analysis.delete_withdraw_updates(pd.DataFrame(
    #         {'TIME': ['1', '2', '3', '3', '4', '5'],
    #          'TYPE': ['W', 'A', 'W', 'A', 'W', 'W'],
    #          'MONITOR': ['192.168.0.0', '192.168.0.0', '192.168.0.0', '192.168.0.0', '192.168.0.0', '192.168.0.0'],
    #          'AS': ['123', '6001', '342', '1231', '3254', '43265'],
    #          'PREFIX': ['193.169.130.0/23', '118.184.200.0/21', '118.184.224.0/21', '118.184.232.0/21',
    #                     '78.130.255.0/24', '213.145.111.0/24'],
    #          'AS_PATH': ["['45896', '6939', '15412', '29357', '43852', '43852', '43852', '43852', '43852', '21050']",
    #                      "['45896', '3356', '174', '20473', '18257']", "['45896', '3356', '174', '20473', '18257']",
    #                      "['45896', '3356', '174', '20473', '18257']", "['45896', '3356', '60262', '207214']",
    #                      "['45896', '3356', '60262', '207214']"]}))
    #
    #     expected_result = pd.DataFrame(
    #         {'TIME': ['2', '3'],
    #          'TYPE': ['A', 'A'],
    #          'MONITOR': ['192.168.0.0', '192.168.0.0'],
    #          'AS': ['6001', '1231'],
    #          'PREFIX': ['118.184.200.0/21', '118.184.232.0/21'],
    #          'AS_PATH': ["['45896', '3356', '174', '20473', '18257']",
    #                      "['45896', '3356', '174', '20473', '18257']"]})
    #
    #     self.assertEqual(result['TYPE'].values.all(), expected_result['TYPE'].values.all())
    #
    # def test_get_IPv4_prefixes_seen_per_monitor(self):
    #     result = more_specifics_analysis.get_prefixes_seen_per_monitor({'TIME': ['1', '3', '4', '5'],
    #                                                                     'TYPE': ['A', 'A', 'A', 'A'],
    #                                                                     'MONITOR': ['193.0.0.55', '193.0.0.56',
    #                                                                                 '193.0.0.56', '193.0.0.56'],
    #                                                                     'AS': ['123', '342', '342', '342'],
    #                                                                     'PREFIX': ['118.184.200.0/21',
    #                                                                                '118.184.200.0/21',
    #                                                                                '78.130.255.0/24',
    #                                                                                '213.145.111.0/24'],
    #                                                                     'AS_PATH': [
    #                                                                         "['45896', '6939', '15412', '29357', '43852', '43852', '43852', '43852', '43852', '21050']",
    #                                                                         "['45896', '3356', '174', '20473', '18257']",
    #                                                                         "['45896', '3356', '174', '20473', '18257']",
    #                                                                         "['45896', '3356', '174', '20473', '18257']"]})
    #
    #     expected_result = {'193.0.0.55': ['118.184.200.0/21'],
    #                        '193.0.0.56': ['118.184.200.0/21',
    #                                       '78.130.255.0/24',
    #                                       '213.145.111.0/24']}
    #
    #     self.assertEqual(result, expected_result)
    #
    # def test_get_IPv6_prefixes_seen_per_monitor(self):
    #     result = more_specifics_analysis.get_prefixes_seen_per_monitor({'TIME': ['1', '3', '4', '5'],
    #                                                                     'TYPE': ['A', 'A', 'A', 'A'],
    #                                                                     'MONITOR': ['2a01:2a8::3', '193.0.0.56',
    #                                                                                 '193.0.0.56', '193.0.0.56'],
    #                                                                     'AS': ['123', '342', '342', '342'],
    #                                                                     'PREFIX': ['2001:7fb:fe05::/48',
    #                                                                                '2a06:e881:1400::/47',
    #                                                                                '2a06:e881:1400::/47',
    #                                                                                '2001:7fb:fe05::/48'],
    #                                                                     'AS_PATH': [
    #                                                                         "['45896', '6939', '15412', '29357', '43852', '43852', '43852', '43852', '43852', '21050']",
    #                                                                         "['45896', '3356', '174', '20473', '18257']",
    #                                                                         "['45896', '3356', '174', '20473', '18257']",
    #                                                                         "['45896', '3356', '174', '20473', '18257']"]})
    #
    #     expected_result = {'2a01:2a8::3': ['2001:7fb:fe05::/48'],
    #                        '193.0.0.56': ['2a06:e881:1400::/47', '2001:7fb:fe05::/48']}
    #
    #     self.assertEqual(result, expected_result)
    #
    # def test_count_prefixes_per_monitor(self):
    #     monitors, counts_per_monitor = more_specifics_analysis.count_prefixes_per_monitor(
    #         {'2a01:2a8::3': ['2001:7fb:fe05::/48'],
    #          '193.0.0.56': ['2a06:e881:1400::/47',
    #                         '2001:7fb:fe05::/48']})
    #
    #     self.assertEqual(monitors, ['193.0.0.56', '2a01:2a8::3'])
    #     self.assertEqual(counts_per_monitor, [2, 1])
    #
    # def test_cluster_advises_per_monitor(self):
    #     least_specifics, more_specifics, intermediates, uniques = more_specifics_analysis.cluster_advises_per_monitor(
    #         {'193.0.0.55': ['118.184.200.0/31',
    #                         '118.184.200.0/32',
    #                         '118.184.200.1/32',
    #                         '118.184.200.0/30'],
    #          '193.0.0.56': ['118.184.200.0/21',
    #                         '78.130.255.0/24',
    #                         '213.145.111.0/24']})
    #     expected_least_specifics = {'193.0.0.55': ['118.184.200.0/30'],
    #                                 '193.0.0.56': []}
    #
    #     self.assertEqual(expected_least_specifics, least_specifics)
    #
    #     expected_more_specifics = {'193.0.0.55': ['118.184.200.0/32',
    #                                               '118.184.200.1/32'],
    #                                '193.0.0.56': []}
    #
    #     self.assertEqual(more_specifics, expected_more_specifics)
    #
    #     expected_intermediates = {'193.0.0.55': ['118.184.200.0/31'],
    #                               '193.0.0.56': []}
    #
    #     self.assertEqual(intermediates, expected_intermediates)
    #
    #     expected_uniques = {'193.0.0.55': [],
    #                         '193.0.0.56': ['118.184.200.0/21',
    #                                        '78.130.255.0/24',
    #                                        '213.145.111.0/24']}
    #
    #     self.assertEqual(expected_uniques, uniques)


if __name__ == '__main__':
    unittest.main()
