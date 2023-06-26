import configparser

config = configparser.ConfigParser()
config.read('config.ini')



from elasticsearch import Elasticsearch
from pprint import pprint
client = Elasticsearch(
       cloud_id=config['ELASTIC']['cloud_id'],
       basic_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)
"""The following queries delete the documents who have unit prices below 0 and above 500"""
query = {
  "query":{
    "range":{
      "UnitPrice":{
        "lte":0
      }
    }
  }
}

query1 = {
  "query":{
    "range":{
      "UnitPrice":{
        "gte":500
      }
    }
  }
}

result = client.delete_by_query(index='ecommerce_data',body=query)

"""The following query finds sum of the unitprices. Similarly when sum is replaced by min, max, avg is respective
mathematical operations are carried out"""
query2 = {
  "aggs": {
    "index_sum": {
      "sum": {
        "field": "UnitPrice"
      }
    }
  }
}

"""the query below gives all the above mathematical operation in one command"""
query3 = {
  "aggs": {
    "all_stats_unit_price": {
      "stats": {
        "field": "UnitPrice"
      }
    }
  }
}


"""The below query gives no of unique customer ids"""
query4 = {
  "aggs": {
    "unique_orders": {
      "cardinality": {
        "field": "CustomerID"
      }
    }
  }
}

"""The following query gives you documents whose country is germany with average unit price in that country"""
query5 = {
  "query": {
    "match": {
      "Country": "Germany"
    }
  },
  "aggs": {
    "germany_avg_price": {
      "avg": {
        "field": "UnitPrice"
      }
    }
  }
}

"""The following query gives documents that have 8 hrs difference between invoice dates. The calender interval
property can be replaced with fixed interval which has a smaller time gap. Also desc can be replaced with asc"""
query6 = {
  "size": 0,
  "aggs": {
    "transactions_by_month": {
      "date_histogram": {
        "field": "InvoiceDate",
        "calendar_interval": "1M",
        "order": {
          "_key": "desc"
        }
      }
    }
  }
}

"""The query gives you which has unitprice in intervals of 10 and its counts"""
query7 = {
  "aggs": {
    "transactions_per_price_interval": {
      "histogram": {
        "field": "UnitPrice",
        "interval": 10
      }
    }
  }
}

"""THe following query gives you unit price counts with specific ranges"""
query8 = {
  "aggs": {
    "transactions_per_custom_price_interval": {
      "range": {
        "field": "UnitPrice",
        "ranges": [
          {
            "to": 50
          },
          {
            "from": 50,
            "to": 200
          },
          {
            "from": 200
          }
        ]
      }
    }
  }
}

# pprint(client.search(index="ecommerce_data",body=query8))

"""The following query return top 5 customers who have bought most items"""
query9 = {
  "aggs": {
    "top_5_customers": {
      "terms": {
        "field": "CustomerID",
        "size": 5
      }
    }
  }
}

"""The following query gives us transactions per day from highest to lowest. We calculate daily revenue using
Unit price and quantity and no of unique customers."""
query10 = {
  "aggs": {
    "transactions_per_day": {
      "date_histogram": {
        "field": "InvoiceDate",
        "calendar_interval": "day",
        "order": {
          "daily_revenue": "desc"
        }
      },
      "aggs": {
        "daily_revenue": {
          "sum": {
            "script": {
              "source": "doc['UnitPrice'].value * doc['Quantity'].value"
            }
          }
        },
        "number_of_unique_customers_all_day":{
          "cardinality": {
            "field": "CustomerID"
          }
        }
      }
    }
  }
}