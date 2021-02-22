#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import datetime
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search

### Script que toma datos de un indice de elasticsearch
### crea un df nuevo a partir de estos datos y los inserta en un indice nuevo.

### Credenciales
client = Elasticsearch(["---"],
                        http_auth=("---"),
                        use_ssl=True)


try:
  ### Ejemplo de query. Deleteo de datos en rango de fecha
  s = Search(using=client, index='index').query('range' ,  **{'Date': {'gte': "now-14d/d" , "lte": "now-3d/d"}})
  response = s.delete()
except:
  pass


###
now = datetime.datetime.now()
now1 = now - datetime.timedelta(days=14)
now2 = now - datetime.timedelta(days=3)
fechas = pd.date_range(start=now1, end=now2)

### Testeos
# fechas = pd.date_range(start="2021-01-01", end="2021-03-01")


for i in fechas:

    ### Origen de datos
    s = Search(using=client, index="index") \
        .filter('range' ,  **{'Date': {'gte': i.strftime("%Y-%m-%d") , "lte": i.strftime("%Y-%m-%d")}})

    ### Creando el DF
    df = pd.DataFrame((d.to_dict() for d in s.scan()))

    ### Procesamiento de DF
    df['Count_Docs'] = df.shape[0]

    df = json.loads(df.to_json(orient='records'))

    ### Creacion del indice e insercion de datos
    actions = [
    {
        "_index": "oci-nc-agg",
        "_source":i,
    }
        for i in df
    ]
    helpers.bulk(client, actions)
