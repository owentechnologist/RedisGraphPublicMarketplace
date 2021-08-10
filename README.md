# RedisGraphPublicMarketplace
This graph records the postings of participants in a product and services marketplace.  They 'love' items posted and it shows location of items and participants and creates 'near' relationships to major cities. (in Canada)

Cities data is loaded from a csv file populated with data from a free data set provided by: https://simplemaps.com/

Comments in posts come from french and english sample text files.  (they do not make any realistic sense)

Querying the graph results in rich results:
![image](https://user-images.githubusercontent.com/48262631/128918126-4bd0b251-e317-48a9-b018-fb60e7d72004.png)

And the data can hold some useful surprises:

GRAPH.QUERY "recommendations" "CALL db.idx.fulltext.queryNodes('Post', 'erotica') YIELD node as badPost MATCH (b)--(i)--(p:Post)-[:POSTED_BY]->(m:Member) WHERE p.id = badPost.id RETURN b,i,p, m"


![image](https://user-images.githubusercontent.com/48262631/128919784-7bb92370-cc7c-4224-914d-159a07aca4fe.png)


