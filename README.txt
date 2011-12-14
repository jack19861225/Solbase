INTRODUCTION

Solbase is highly scalable search engine built on top of Lucene, Solr, and HBase.
In a nut shell, Solbase replaced Lucene's index file layer that was stored in local file system
under NFS to big database implementation in HBase.

Leveraging HBase's high performance scan, inverted index is efficiently stored in HBase for
fast data retrieval.

MAIN HIGHLIGHTS OF SOLBASE

Because HBase's ability to scale and distribute large data in distributed fashion, Solbase can scale and
at the same time it perform very well with local in memory cache.

Solbase also inherited Solr's distributed processing layer for sharding large index into smaller pieces.

It also overcame Lucene & Solr's limitation with sortable fields. Instead of creating array of all docs for sorting/filtering,
values are stored at index. therefore, Solbase can fetch those vals on the fly.

DEPENDENCIES

By overcoming Lucene & Solr's limitations, Solbase had to forked off of Lucene and Solr repos for modifications and improvements in its cores.
Solbase-Lucene & Solbase-Solr packages are included in this repository, but can be found at:

https://github.com/Photobucket/Solbase-Solr
https://github.com/Photobucket/Solbase-Lucene


