from abc import abstractmethod, ABC
from functools import lru_cache

import pandas as pd
from loguru import logger
from pymongo import MongoClient
import streamlit as st
from typing import *
import plotly.express as px


class DataFetcher:
    def __init__(self, modality: str, mongodb_connection_string: str, db_name: str):
        self.modality = modality
        self._mongodb_connection_string = mongodb_connection_string
        self._db_name = db_name

    @lru_cache
    def get_name_specific_dataframe(self, collection_name: str, name: str) -> pd.DataFrame:
        with MongoClient(self._mongodb_connection_string) as client:
            db = client[self._db_name]

            result = db[collection_name].aggregate(self.pipeline_for_name_specific_values(name=name))

        return pd.DataFrame(result)

    def get_collection_as_dataframe(self, collection_name: str):
        with MongoClient(self._mongodb_connection_string) as client:
            db = client[self._db_name]

            result = db[collection_name].aggregate(self.pipeline_for_collection_to_dataframe())

            return pd.DataFrame(result)

    @staticmethod
    def pipeline_for_collection_to_dataframe():
        return [
            {
                '$project': {
                    'metadata': 0,
                    '_id': 0
                }
            }
        ]

    @lru_cache
    def get_all_names_in_a_collection(self, collection_name: str) -> List[str]:
        with MongoClient(self._mongodb_connection_string) as client:
            db = client[self._db_name]
            return sorted(db[collection_name].distinct('name'))

    @staticmethod
    def pipeline_for_name_specific_values(name: str):
        return [
            {
                '$project': {
                    'metadata': 0,
                    '_id': 0
                }
            }, {
                '$match': {
                    'name': name
                }
            }
        ]

    def __eq__(self, other):
        return self.modality == other.modality and self._mongodb_connection_string == other._mongodb_connection_string and self._db_name == other._db_name

    def __hash__(self):
        return hash((self._mongodb_connection_string, self._db_name, self.modality))

    @lru_cache
    def get_variance_for_all_names(self, collection_name: str) -> pd.DataFrame:
        return pd.DataFrame(
            [{'var': self.get_name_specific_dataframe(collection_name=collection_name, name=name).name.var(),
              'name': name} for name in
             self.get_all_names_in_a_collection(collection_name)])


class AbstractDashboard(DataFetcher, ABC):
    def __init__(self, mongodb_connection_string: str, db_name: str):
        modality = st.sidebar.text_input(label='Modality Name')
        if not modality:
            st.sidebar.info('Please input modality')
            st.stop()

        super().__init__(modality, mongodb_connection_string, db_name)

    @abstractmethod
    def render(self):
        ...


class VarianceDescription(AbstractDashboard):
    def render(self):
        variance_table = self.get_variance_for_all_names(collection_name=self.modality)
        st.table(variance_table.describe())


class BoxPlotRenderer(AbstractDashboard):
    def render(self):
        df = self.get_collection_as_dataframe(collection_name=self.modality)
        fig = px.box(df, x='name', y='value', width=1000)
        st.plotly_chart(fig)


class MyRenderer(VarianceDescription, BoxPlotRenderer): pass


def main(
        mongodb_connection_string: str = "mongodb://omics-reader:MongoDb-4a61d6b3befc019d76133@132.66.207.18:80/TCGAOmics?authSource=admin",
        db_name: str = "TCGAOmics"
):
    MyRenderer(mongodb_connection_string=mongodb_connection_string, db_name=db_name).render()


if __name__ == '__main__':
    main()
