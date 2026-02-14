# Databricks notebook source
from asset_bundles_gitHub_actions.main import main

# Simulamos os argumentos que o Job passaria
# Use catálogos/schemas que existam no seu ambiente de teste
args_teste = ["--catalog", "teste_lakeflow_connect", "--schema", "marlonzucolotto"]

# Executa a função main passando os argumentos
df_resultado = main(args_teste)

# Validações reais
assert df_resultado is not None, "A função main retornou None"
assert df_resultado.count() > 0, "O DataFrame retornado está vazio"

print("✅ Teste unitário (Main) passou com sucesso!")