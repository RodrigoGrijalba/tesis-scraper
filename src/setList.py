from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
import pandas as pd

URL = "https://tesis.pucp.edu.pe/oai/request"

def main():
        registry = MetadataRegistry()
        registry.registerReader(
                'oai_dc',
                oai_dc_reader
        )
        client = Client(URL, registry)
        setNames = []
        setCodes = []

        for record in client.listSets():
                setNames += [record[1]]
                setCodes += [record[0]]

        setList = pd.DataFrame({
                "name": setNames,
                "code": setCodes
        })
        setList.to_csv(
                "data/setList.csv",
                encoding="utf-8-sig"
        )

if __name__ == "__main__":
        main()
