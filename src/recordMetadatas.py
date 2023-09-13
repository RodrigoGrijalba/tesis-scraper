import pandas as pd
import requests
import bs4
import os

dataPaths = [f"data/items/{file}" for file in os.listdir("data/items")]
metadataFields = [
        "dc.contributor.advisor",
        "dc.contributor.author",
        "dc.date.created",
        "dc.identifier.uri",
        "dc.title", 
        "dc.description.abstract",
        "thesis.degree.discipline"
]
outputFileName = "recordMetadatas"

def getItemMetadataLinks(file):
        collectionItemLinks = pd.read_xml(file)
        metadataLinks = list(collectionItemLinks.link.values[:])
        return metadataLinks

def metadataFromRequest(metadataRequest):
        metadata = pd.json_normalize(metadataRequest.json())
        filteredMetadata = metadata[metadata.key.isin(metadataFields)][["key", "value"]]
        filteredMetadata = filteredMetadata.groupby(["key"]).agg(lambda col: "\n".join(col)).reset_index()
        filteredMetadata.index = filteredMetadata["key"]
        filteredMetadata = pd.DataFrame([filteredMetadata.T.reset_index().iloc[1, 1:]])
        return filteredMetadata


def main():
        output = pd.DataFrame()
        # dataframe creation for each path
        itemMetadataLinks = []
        for file in dataPaths:
                itemMetadataLinks += getItemMetadataLinks(file)
        
        print(f"Fetching metadata for all {len(itemMetadataLinks)} entries. This may take a while")
        print(" ---+--- 1 ---+--- 2 ---+--- 3 ---+--- 4 ---+--- 5")

        for index, link in enumerate(itemMetadataLinks):
                print(".", end="")
                if (index + 1) % 50 == 0:
                        print("     ", index + 1, sep="")
                metadataRequest = requests.get(f"https://tesis.pucp.edu.pe{link}/metadata")
                # dict with desired link contents
                filteredMetadata = metadataFromRequest(metadataRequest)
                output = pd.concat([output, filteredMetadata])
        
        output = output.reset_index(drop=True)
        output.columns = pd.Index([column.split(".")[-1] for column in output.columns])
        output.to_csv(f"data/{outputFileName}.csv", encoding="utf-8-sig")
        # save final df

if __name__ == "__main__":
        main()
