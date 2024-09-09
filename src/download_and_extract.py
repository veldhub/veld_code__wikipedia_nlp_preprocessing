import json
import os
import subprocess

import yaml


IN_TMP_PATH = os.getenv("tmp_extracted_path")
OUT_JSON_PATH = "/veld/output/data/"
OUT_VELD_DATA_YAML_PATH = "/veld/output/veld_data_extracted.yaml"


veld_data_yaml = {
    "x-veld": {
        "data": {
            "description": "extracted wikipedia data, where each json file is one article.",
            "topics": "NLP",
            "contents": [
                "training data",
                "raw text",
            ],
            "file_type": "json",
            "additional": {
                "data size": None,
            }
        }
    }
}


def transform():

    def recurse_files(path, n):
        for sub_path in os.listdir(path):
            abs_sub_path = path + "/" + sub_path
            if os.path.isdir(abs_sub_path):
                n = recurse_files(abs_sub_path, n)
            else:
                with open(abs_sub_path, "r") as f_in:
                    for line in f_in:
                        article = json.loads(line)
                        article_path = OUT_JSON_PATH + article["id"] + ".json"
                        if os.path.exists(article_path):
                            print(f"overwriting: {article_path}", flush=True)
                        with open(article_path, "w") as f_out:
                            json.dump(article, f_out, ensure_ascii=False)
                            n += 1
        return n

    def transform_main():
        print("transforming extracted wikipedia data into distinct json files.", flush=True)
        if not os.path.exists(OUT_JSON_PATH):
            os.mkdir(OUT_JSON_PATH)
        n = recurse_files(IN_TMP_PATH, 0)
        result = subprocess.run(["du", "-sh", OUT_JSON_PATH], capture_output=True, text=True)
        data_size = result.stdout.split()[0]
        veld_data_yaml["x-veld"]["data"]["additional"]["data size"] = data_size
        print(f"done. number of extracted files: {n}, size of data: {data_size}", flush=True)

    transform_main()


def write_veld_data_yaml():
    print("writing veld data yaml file.", flush=True)
    with open(OUT_VELD_DATA_YAML_PATH, "w") as f:
        yaml.dump(veld_data_yaml, f, sort_keys=False)
    print("done.", flush=True)
    

if __name__ == "__main__":
    transform()
    write_veld_data_yaml()

