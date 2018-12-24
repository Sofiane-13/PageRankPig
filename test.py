#!/usr/bin/python 
# -*- coding: utf-8 -*-
# explicitly import Pig class 
from org.apache.pig.scripting import Pig 

# COMPILE: compile method returns a Pig object that represents the pipeline
P = Pig.compile('''Arcs = LOAD '$docs_in'  USING PigStorage('\t') AS (url: chararray, pagerank: float, links:{ link: ( url: chararray ) } );   
        outlinkPageRank =  FOREACH Arcs    GENERATE   pagerank / COUNT ( links ) AS pagerank, FLATTEN ( links ) AS to_url;
        newPageRank = FOREACH   ( COGROUP outlinkPageRank BY to_url, Arcs BY url INNER )   GENERATE  
        FLATTEN (Arcs.url),
        ( 1.0 - 0.85 ) + 0.85 * SUM ( outlinkPageRank.pagerank ) AS pagerank,
	 FLATTEN (Arcs.links) AS links;
	dump newPageRank;
	STORE newPageRank INTO '$docs_out';''')
params = {'docs_in': 'urls2.txt' }
for i in range(1):
   out = "out/pagerank_data_" + str(i + 1)
   params["docs_out"] = out
   Pig.fs("rmr " + out)	
   stats = P.bind(params).runSingle()
   if not stats.isSuccessful():
      raise 'failed'
   params["docs_in"] = out
