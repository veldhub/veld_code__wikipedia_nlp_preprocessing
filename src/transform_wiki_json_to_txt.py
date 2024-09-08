import json
import os

import spacy


IN_JSON_FILES_PATH = "/veld/input/"
OUT_TXT_PATH = "/veld/output/" + os.getenv("out_txt_file")
nlp = spacy.load("de_core_news_lg")


def transform():
    with open(OUT_TXT_PATH, "w") as f_out:
        for file in os.listdir(IN_JSON_FILES_PATH):
            with open(IN_JSON_FILES_PATH + file, "r") as f_in:
                data = json.load(f_in)
                text = data["text"].replace("\n", " ")
                doc = nlp(text)
                for sent in doc.sents:
                    f_out.write(sent.text + "\n")


if __name__ == "__main__":
    transform()

