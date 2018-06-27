import unittest2 as unittest
import more_specifics_analysis


class TestMoreSpecificsAnalysis(unittest.TestCase):

    def test_get_IPv4_prefixes_seen_per_monitor(self):
        result = more_specifics_analysis.get_prefixes_seen_per_monitor({'TIME': ['1', '3', '4', '5'],
                                                                        'TYPE': ['A', 'A', 'A', 'A'],
                                                                        'MONITOR': ['193.0.0.55', '193.0.0.56',
                                                                                    '193.0.0.56', '193.0.0.56'],
                                                                        'AS': ['123', '342', '342', '342'],
                                                                        'PREFIX': ['118.184.200.0/21',
                                                                                   '118.184.200.0/21',
                                                                                   '78.130.255.0/24',
                                                                                   '213.145.111.0/24'],
                                                                        'AS_PATH': [
                                                                            "['45896', '6939', '15412', '29357', '43852', '43852', '43852', '43852', '43852', '21050']",
                                                                            "['45896', '3356', '174', '20473', '18257']",
                                                                            "['45896', '3356', '174', '20473', '18257']",
                                                                            "['45896', '3356', '174', '20473', '18257']"]})

        expected_result = {'193.0.0.55': ['118.184.200.0/21'],
                           '193.0.0.56': ['118.184.200.0/21',
                                          '78.130.255.0/24',
                                          '213.145.111.0/24']}

        self.assertEqual(result, expected_result)

    def test_get_IPv6_prefixes_seen_per_monitor(self):
        result = more_specifics_analysis.get_prefixes_seen_per_monitor({'TIME': ['1', '3', '4', '5'],
                                                                        'TYPE': ['A', 'A', 'A', 'A'],
                                                                        'MONITOR': ['2a01:2a8::3', '193.0.0.56',
                                                                                    '193.0.0.56', '193.0.0.56'],
                                                                        'AS': ['123', '342', '342', '342'],
                                                                        'PREFIX': ['2001:7fb:fe05::/48',
                                                                                   '2a06:e881:1400::/47',
                                                                                   '2a06:e881:1400::/47',
                                                                                   '2001:7fb:fe05::/48'],
                                                                        'AS_PATH': [
                                                                            "['45896', '6939', '15412', '29357', '43852', '43852', '43852', '43852', '43852', '21050']",
                                                                            "['45896', '3356', '174', '20473', '18257']",
                                                                            "['45896', '3356', '174', '20473', '18257']",
                                                                            "['45896', '3356', '174', '20473', '18257']"]})

        expected_result = {'2a01:2a8::3': ['2001:7fb:fe05::/48'],
                           '193.0.0.56': ['2a06:e881:1400::/47', '2001:7fb:fe05::/48']}

        self.assertEqual(result, expected_result)

    def test_count_prefixes_per_monitor(self):
        monitors, counts_per_monitor = more_specifics_analysis.count_prefixes_per_monitor(
            {'2a01:2a8::3': ['2001:7fb:fe05::/48'],
             '193.0.0.56': ['2a06:e881:1400::/47',
                            '2001:7fb:fe05::/48']})

        self.assertEqual(monitors, ['193.0.0.56', '2a01:2a8::3'])
        self.assertEqual(counts_per_monitor, [2, 1])

    def test_cluster_advises_per_monitor(self):
        least_specifics, more_specifics, intermediates, uniques = more_specifics_analysis.cluster_advises_per_monitor(
            {'193.0.0.55': ['118.184.200.0/31',
                            '118.184.200.0/32',
                            '118.184.200.1/32',
                            '118.184.200.0/30'],
             '193.0.0.56': ['118.184.200.0/21',
                            '78.130.255.0/24',
                            '213.145.111.0/24']})
        expected_least_specifics = {'193.0.0.55': ['118.184.200.0/30'],
                                    '193.0.0.56': []}

        self.assertEqual(expected_least_specifics, least_specifics)

        expected_more_specifics = {'193.0.0.55': ['118.184.200.0/32',
                                                  '118.184.200.1/32'],
                                   '193.0.0.56': []}

        self.assertEqual(more_specifics, expected_more_specifics)

        expected_intermediates = {'193.0.0.55': ['118.184.200.0/31'],
                                  '193.0.0.56': []}

        self.assertEqual(intermediates, expected_intermediates)

        expected_uniques = {'193.0.0.55': [],
                            '193.0.0.56': ['118.184.200.0/21',
                                           '78.130.255.0/24',
                                           '213.145.111.0/24']}

        self.assertEqual(expected_uniques, uniques)


if __name__ == '__main__':
    unittest.main()
