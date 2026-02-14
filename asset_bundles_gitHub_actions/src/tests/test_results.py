# Databricks notebook source
#from asset_bundles_gitHub_actions.tests.conftest import spark

df1 = spark.sql("SELECT COUNT(*) AS total_1 FROM teste_lakeflow_connect.marlonzucolotto.sample_trips_asset_bundles_github_actions")
total1 = df1.collect()[0]["total_1"]

assert total1 > 0, "Tabela está vazia"
assert total1 == 21932, f"Esperado 21932 registros, encontrado {total1}"

print("✅ Validação de dados passou")

df2 = spark.sql("SELECT COUNT(*) AS total_2 FROM teste_lakeflow_connect.marlonzucolotto.sample_zones_asset_bundles_github_actions")
total2 = df2.collect()[0]["total_2"]

assert total2 > 0, "Tabela está vazia"
assert total2 == 128, f"Esperado 128 registros, encontrado {total2}"

print("✅ Validação de dados passou")