import streamlit as st
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from datetime import datetime, timedelta

class PriceMonitor:
    def __init__(self):
        self.spark = SparkSession.builder.appName("PriceMonitor").getOrCreate()
        self.data = self.load_data()

    @staticmethod
    @st.cache
    def load_data():
        # Aqui você carregaria os dados reais
        # Exemplo com dados fictícios
        data = {
            'product_id': [1, 1, 1, 2, 2, 2, 3, 3, 3],
            'store_id': [1, 1, 1, 2, 2, 2, 3, 3, 3],
            'price': [100, 90, 80, 200, 180, 160, 300, 270, 250],
            'date': pd.date_range(start='2021-01-01', periods=9, freq='M')
        }
        df = pd.DataFrame(data)
        return df

    def filter_data(self, num_months, percentual_economia):
        end_date = datetime.now()
        start_date = end_date - timedelta(days=num_months*30)
        
        sdf = self.spark.createDataFrame(self.data)
        filtered_sdf = sdf.filter((col('date') >= start_date) & (col('date') <= end_date))
        
        avg_prices = filtered_sdf.groupBy('product_id').agg(avg('price').alias('avg_price'))
        filtered_avg_prices = avg_prices.filter((col('avg_price') * (1 - percentual_economia / 100)) >= col('avg_price'))
        
        result_df = filtered_avg_prices.toPandas()
        return result_df

    def display(self):
        st.title("Monitor de Preços")
        
        num_months = st.number_input("Número de meses a serem considerados", min_value=1, max_value=12, value=3)
        percentual_economia = st.number_input("Percentual de economia buscado (%)", min_value=0, max_value=100, value=20)
        
        result_df = self.filter_data(num_months, percentual_economia)
        
        if not result_df.empty:
            st.write("Produtos encontrados:")
            st.write(result_df)
        else:
            st.write("Nenhum produto encontrado com os critérios especificados.")

# Instanciando e exibindo o monitor de preços
monitor = PriceMonitor()
monitor.display()
