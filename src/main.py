from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.embeddings import OpenAIEmbeddings
import pandas as pd
import os
import pinecone
import uuid
import itertools
import numpy
import tiktoken
from dotenv import load_dotenv
from count_tokens import count_tokens

load_dotenv()

tokenizer = tiktoken.get_encoding("cl100k_base")
index_name = "test"
embeddings = OpenAIEmbeddings()
text_splitter = RecursiveCharacterTextSplitter(
        chunk_size = 512,
        chunk_overlap = 24,
        length_function = count_tokens
)

def embedFromTextsAndMetadatas(texts, metadatas):
        vectorsWithMetadata = []
        embeddedTexts = embeddings.embed_documents(texts)
        hashes = [str(uuid.uuid4()) for _ in texts]
        for i, (text, embeddedText) in enumerate(zip(texts, embeddedTexts)):
                textMetadata = metadatas[i]
                textMetadata["text"] = text
                vectorsWithMetadata += [(hashes[i], embeddedText, textMetadata)]
        
        return vectorsWithMetadata

def createBatchOfEmbeddings(embeddedTexts, batchSize):
    iterateOverEmbeddings = iter(embeddedTexts)
    embeddingChunk = itertools.islice(iterateOverEmbeddings, batchSize)
    
    while embeddingChunk:
        yield embeddingChunk
        embeddingChunk = tuple(itertools.islice(iterateOverEmbeddings, batchSize))

def batchUpsertEmbeddings(embeddedTexts, batchSize, upsertIndex):
        for batch in createBatchOfEmbeddings(docEmbeddings, batchSize):
                upsertIndex.upsert(
                vectors=batch
                )

def main():
        pinecone.init()
        metadataEntries = pd.read_csv("data/recordMetadatas.csv").drop(columns=["Unnamed: 0"])
        metadataEntries = metadataEntries.fillna("No Disponible")
        textChunks = []
        textChunkMetadatas = []
        for index, entry in metadataEntries.iterrows():
                newChunks = text_splitter.split_text(entry["abstract"])
                newMetadatas = [{key: entry[key] for key in entry.drop(index=["abstract"]).index} for chunk in newChunks]
                textChunks += newChunks
                textChunkMetadatas += newMetadatas
        
        if index_name not in pinecone.list_indexes():
                pinecone.create_index(
                        name = index_name,
                        metric = "cosine",
                        dimension = 1536
                )
        
        pineconeIndex = pinecone.Index(index_name)

        embeddedTexts = embedFromTextsAndMetadatas(textChunks, textChunkMetadatas)
        print(embeddedTexts)

        batchUpsertEmbeddings(embeddedTexts, 100, pineconeIndex)

if __name__ == "__main__":
        main()