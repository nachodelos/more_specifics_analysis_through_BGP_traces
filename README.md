# more_specifics_analysis_through_BGP_traces

Nowadays, Internet is the most important communication technology in global terms. It presents an old technology, but it is not easy to know how operates due to its distributed constitution. Internet is constantly evolving and growing. It is essential to ensure connectivity and reachability among connected devices. This task is carried on by routing protocols. BGP is a standardized exterior gateway protocol (EGP).

The proposed study analyses data that has been interchanged among a set of nodes. The data used for this project was acquired at the RIPE NCC database. The analysis is focused on more specifics prefixes which have been learnt by individual nodes. For each prefix, it has been extracted the following features: length, deep, number of updates and visibility.

Regarding prefixes distribution, not every node learns the same number of prefixes. Those nodes that depict providers give a good perspective about protocol operation. These nodes are composed mostly of more specific prefixes (around 51%). In addition, most of these prefixes have a length greater than /22s, although most of them are /24s. For each root prefix, there is a mean of 8 more specific prefixes. Furthermore, the most typical deep is 8 and most of more specific prefixes have a single level of disaggregation, which indicates that it is being advertised only a little fraction of prefixes for each root prefix.

As for visibility, more specifics represent the least stable prefixes in contrast with the rest of the prefixes. There is a relationship between prefix lengths and visibility, which is reflected in more specifics such as network filters or connectivity loss.

Keywords: Routing protocols, BGP, prefixes, more specifics, longest prefix match, visibility, RIPE NCC, Python, Tableau.

