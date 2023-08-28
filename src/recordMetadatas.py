import pandas as pd
from urllib.request import urlretrieve
from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader

# TODO: add all sets listed in Header for each record to metadata

URL = "https://tesis.pucp.edu.pe/oai/request"
setList = pd.read_csv(
        "data/setList.csv", 
        encoding="utf-8-sig"
)
setNames = [
        "Economía (Lic.)", 
        "Ciencia Política y Gobierno (Lic.)"
]

def cleanMetadataEntries(recordMetadata):
        for key, value in recordMetadata.items():
                if len(value) > 1:
                        recordMetadata[key] = "\n".join(value)
                
                if len(value) == 0:
                        recordMetadata[key] = "Missing"
        
        return recordMetadata

def DataFramesFromClient(client, setCode, collection):
        metadata = pd.DataFrame()
        # header = pd.DataFrame()

        for record in client.listRecords(
                metadataPrefix='oai_dc', 
                set = setCode
        ):
                recordMetadata = record[1].getMap()
                recordMetadata = cleanMetadataEntries(recordMetadata)
                recordMetadata = pd.DataFrame.from_dict(recordMetadata)
                recordMetadata.fillna("", inplace=True)
                recordMetadata["collection"] = collection
                metadata = pd.concat([metadata, recordMetadata])
        
        return metadata

def main():
        recordMetadatas = pd.DataFrame()

        for collection in setNames:
                setCode = setList[setList["name"] == collection]["code"].values[0]
                print(setCode)
                registry = MetadataRegistry()
                registry.registerReader('oai_dc', oai_dc_reader)
                client = Client(URL, registry)
                collectionMetadata = DataFramesFromClient(client, setCode, collection)
                recordMetadatas = pd.concat([recordMetadatas, collectionMetadata])
        
        recordMetadatas.to_csv("data/recordMetadatas.csv", encoding="utf-8-sig")

if __name__ == "__main__":
        main()