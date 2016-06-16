searchViewCount.pig

===================

This is the first step of the Popular Search workflow.  After the search and view page events are collected and stored in HDFS, this script takes both streams and tries to find keywords that result in page views.  The resulting set of keywords are deemed significant in terms of popularity and are stored.

