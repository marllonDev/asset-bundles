import argparse
import sys
from databricks.sdk.runtime import spark
from asset_bundles_gitHub_actions import taxis

# Adicione 'args=None' para permitir injeção de argumentos no teste
def main(args=None):
    # Se args for None, usa sys.argv (comportamento normal do job)
    # Se args for uma lista (ex: no teste), usa a lista
    parser = argparse.ArgumentParser(
        description="Databricks job with catalog and schema parameters",
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    
    # Processa os argumentos passados ou os do sistema
    parsed_args = parser.parse_args(args)

    # Set the default catalog and schema
    spark.sql(f"USE CATALOG {parsed_args.catalog}")
    spark.sql(f"USE SCHEMA {parsed_args.schema}")

    # Example: just find all taxis from a sample catalog
    df = taxis.find_all_taxis()
    
    # Exibe no log
    df.show(5)
    
    # RETORNA o DataFrame para que o teste possa validar se funcionou
    return df

if __name__ == "__main__":
    main()