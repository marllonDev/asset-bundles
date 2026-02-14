# Databricks notebook source
from asset_bundles_gitHub_actions.main import main

assert main() == "Valor esperado", "Função não retornou o esperado"

print("✅ Teste unitário passou")