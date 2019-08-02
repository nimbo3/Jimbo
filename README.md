# Joojle search engine [![Build Status](https://travis-ci.org/nimbo3/Jimbo.svg?branch=master)](https://travis-ci.org/nimbo3/Jimbo) [![codecov](https://codecov.io/gh/nimbo3/Jimbo/branch/master/graph/badge.svg)](https://codecov.io/gh/nimbo3/Jimbo)
![Jimbo](http://www.staddon.eclipse.co.uk/Logos/jimbo.gif)  
Collaborators:
- Seyyed Mohammad Sadegh Keshavarzi
- Seyyed Alireza Hosseini
- Ali Aliabadi
- Ali Shirmohammadi


## Project modules:  
- commoms (common modules)  
- crawler  
- es_page_processor (process pages for elasticSearch)  
- page_processor (process pages for hbase)  
- search api

## Build with :  
- Spark         - Used to run mapReduces  
- Kafka         - A distrbuted queue that contains 3 main topic (links, page for hbase, page for elasticsearch)  
- ElasticSearch - Used to store data and run search queries  
- Redis         - Used to check politness for domains and check to reduce updating pages for page_processors  
- HBase         - Used to store data about links of a page and anchor
- DropWizard    - Used to monitoring java programs  
- JSoup         - Used to parse the pages  
- Jackson       - Used to serialize and deserializing page class  
- Maven         - Dependency Management  
- Zookeeper     - Used for managing hbase and kafka
- Hadoop        - Used for using proper file system

Check [wikis](https://github.com/nimbo3/jimbo/wiki) for installation of technologies.
