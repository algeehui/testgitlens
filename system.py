# Databricks notebook source
# Skedulo System Layer API Class
# Used to send & receive requests from Skedulo API (Lens & GraphQL) via the corresponding system layer Function App.

# COMMAND ----------

import requests
from pydantic import BaseModel
from typing import Literal

# COMMAND ----------

class WelbeSkedulo:
    def __init__(self):
        self.base_url = dbutils.secrets.get(scope="kv-WB-SCOPE", key="skedulo-system-api-url")
        self.afa_code = dbutils.secrets.get(scope="kv-WB-SCOPE", key="skedulo-system-api-code")

    def get_schema(self) -> requests.Response:
        return requests.get(
            url=self.base_url + "/schema",
            params={"code":self.afa_code}
        )
    
    def get_event_history(self, **params) -> requests.Response:
        params["code"] = self.afa_code
        return requests.get(
            url=self.base_url + "/events/history",
            params=params
        )

    def query_graphql(self, query:str) -> requests.Response:
        return requests.post(
            url=self.base_url + "/graphql/query",
            params={"code":self.afa_code},
            json={"query": query}
        )

    def query_model_by_filter(self, model:BaseModel, filter_value, filter_field:str="UID") -> requests.Response:
        model_name = model.__name__
        response = requests.post(
            url=self.base_url + f"/graphql/query/filter/{model_name}",
            params={"code":self.afa_code},
            json={
                "filter-value": filter_value,
                "filter-field": filter_field
            }
        )
        response.raise_for_status()
        return [model(**{k:v for k,v in entry.items() if v is not None}) for entry in response.json()["modelData"]]

    def upsert_obj(self, model_objs, key_field:str=None, select_fields:list[str]=None) -> requests.Response:
        if type(model_objs) != list:
            model_objs = [model_objs]
        mod_model_name = model_objs[0].__class__.__name__
        if mod_model_name.startswith("New"):
            action = "new"
        elif mod_model_name.startswith("Update"):
            action = "update"
        elif mod_model_name.startswith("Upsert"):
            action = "upsert"
        else:
            raise Exception("Model Object must be of type: New<Object>, Update<Object>, or Upsert<Object>")

        base_model_name = mod_model_name.replace(action.title(), "")

        return requests.post(
            url=self.base_url + f"/graphql/{action}/{base_model_name}",
            params={"code":self.afa_code},
            json={
                "model-objs": [obj.model_dump_json(exclude_none=True) for obj in model_objs],
                "key-field": key_field,
                "select-fields": select_fields
            }
        )

    def duplicate_object(self, model:BaseModel, uid:str, **field_overrides):
        return requests.post(
            url=self.base_url + f"/graphql/duplicate/{model.__name__}/{uid}",
            params={"code":self.afa_code},
            json={"field-overrides": field_overrides}
        )
