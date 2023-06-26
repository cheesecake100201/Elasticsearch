import configparser

config = configparser.ConfigParser()
config.read('config.ini')



from elasticsearch import Elasticsearch
from pprint import pprint
client = Elasticsearch(
       cloud_id=config['ELASTIC']['cloud_id'],
       basic_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
   )
indices = client.indices.get(index="news_headlines")

es_info = client.info()
# print(es_info)

"""This query is used to check the news headlines between two dates"""

query = {
  "query":{
    "range":{
      "date":{
        "gte":"2015-06-20",
        "lte":"2015-09-22"
      }
    }
  }
}
result = client.search(index='news_headlines', body = query)
# pprint(result)
""" This query is used to group the category and find its counts"""
query1 = {
  "aggs":{
    "by_category":{
      "terms": {
        "field": "category",
        "size": 100
      }
    }
  }
}

# pprint(client.search(index='news_headlines', body=query1))
""" This query is used to find out which news headlines fall under category Entertainment and 
then we find the significant text in the headlines of the news we found"""
query2 = {
  "query": {
    "match": {
      "category": "ENTERTAINMENT"
    }
  },
  "aggs": {
    "popular_by_entertainment": {
      "significant_text": {
        "field": "headline"
      }
    }
  }
}

"""The query below is used to find the headline with a higher precision"""

query3 = {
  "query": {
    "match": {
      "headline": {
        "query": "Khloe Kardashian Kendall Jenner",
        "minimum_should_match": 3
      }
    }
  }
}

"""This query is used to match the phrase shape of you in the headline"""
query4 = {
  "query":{
    "match_phrase": {
      "headline": {
        "query": "Shape of you"
      }
    }
  }
}


"""The query below, we search if Michelle Obama is present in the fields in the array"""
query5 = {
  "query": {
    "multi_match": {
      "query": "Michelle Obama",
      "fields": ["short_description", "headline", "authors"]
    }
  }
}


"""The below query searches the dataset for the phrase party planning in the following fields with headline
having more weightage"""
query6 = {
  "query": {
    "multi_match": {
      "query": "Party Planning",
      "fields": ["short_description", "headline^2", "authors"],
      "type": "phrase"
    }
  }
}

"""The following query ask Elasticsearch to query all data that has the phrase "Michelle Obama" in the 
headline. Then, perform aggregations on the queried data and retrieve up to 100 categories that exist in the 
queried data.
"""
query7 = {
  "query": {
    "match_phrase": {
      "headline": "Michelle Obama"
    }
  },
  "aggs": {
    "category_mentions": {
      "terms": {
        "field": "category",
        "size": 100
      }
    }
  }
}

"""The following is a bool query that uses the must clause. This query specifies that all hits must match 
the phrase "Michelle Obama" in the field headline and match the term "POLITICS" in the field category.
"""
query8 = {
  "query": {
    "bool": {
      "must": [
        {"match_phrase": {
          "headline": "Michelle Obama"
        }},
        {
          "match": {
            "category": "POLITICS"
          }
        }
      ]
    }
  }
}

"""The following bool query specifies that all hits must contain the phrase "Michelle Obama" in the field headline. However, 
the hits must_not contain the term "WEDDINGS" in the field category."""
query9 = {
  "query":{
    "bool": {
      "must": [
        {"match_phrase": {
          "headline": "Michelle Obama"
        }}
      ],
      "must_not": [
        {"match": {
          "category": "WEDDINGS"
        }}
      ]
    }
  }
}

"""In the below query, the documents will be shown whose headline will contain michelle obama and if they
lie under the category BLACK VOICES they will be presented as the top searches"""
query10 = {
 "query": {
   "bool": {
     "must": [
       {"match_phrase": {
         "headline": "Michelle Obama"
       }}
     ],
     "should": [
       {"match": {
         "category": "BLACK VOICES"
       }}
     ]
   }
 } 
}

"""The below query finds documents whose headline has michelle obama in it and ranging between specific dates """

query11 = {
  "query": {
    "bool": {
      "must": [
        {"match_phrase": {
          "headline": "Michelle Obama"
        }}
      ],
      "filter": [
        {"range": {
          "date": {
            "gte": "2014-03-25",
            "lte": "2016-03-25"
          }
        }}
      ]
    }
  }
}