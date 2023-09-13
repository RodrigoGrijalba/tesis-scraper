import pandas as pd

collections = pd.read_xml("data/collections.xml")
bashCommand = 'curl -s -H "Accept:application/xml" https://tesis.pucp.edu.pe{link}/items?limit=1000 > "data/items/{name}.xml"'

def main():

        with open("src/get_items.sh", "w", encoding = "utf-8-sig") as bashFile:
                bashFile.write("\n")
                for index, row in collections.iterrows():
                        bashFile.write(bashCommand.format(link = row["link"], name = row["name"]))
                        bashFile.write("\n")

if __name__ == "__main__":
        main()