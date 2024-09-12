import json
import random
import os
import subprocess
from multiprocessing import Process

import spacy
import yaml


IN_JSON_FOLDER_PATH = "/veld/input/" + os.getenv("in_json_folder") + "/"
OUT_TXT_PATH = "/veld/output/" + os.getenv("out_txt_file")
OUT_VELD_DATA_YAML_PATH = "/veld/output/veld_data_transformed.yaml"
OUT_DATA_DESCRIPTION = os.getenv("out_data_description")
CPU_COUNT = os.getenv("cpu_count")
if CPU_COUNT is None:
    CPU_COUNT = os.cpu_count()
else:
    CPU_COUNT = int(CPU_COUNT)
    if CPU_COUNT > os.cpu_count():
        CPU_COUNT = os.cpu_count()
SAMPLE_SIZE_PERCENTAGE = float(os.getenv("sample_size_percentage"))
SAMPLE_RANDOM_SEED = os.getenv("sample_random_seed")
INFO_INTERVAL = int(os.getenv("info_interval"))
SET_SPLIT_SENTENCES = os.getenv("set_split_sentences")
if SET_SPLIT_SENTENCES.lower() == "true":
    SET_SPLIT_SENTENCES = True
else:
    SET_SPLIT_SENTENCES = False
TMP_FILE_FOLDER = "/tmp"
print(f"IN_JSON_FOLDER_PATH: {IN_JSON_FOLDER_PATH}")
print(f"OUT_TXT_PATH: {OUT_TXT_PATH}")
print(f"CPU_COUNT: {CPU_COUNT}")
print(f"SAMPLE_SIZE_PERCENTAGE: {SAMPLE_SIZE_PERCENTAGE}")
print(f"SET_SPLIT_SENTENCES: {SET_SPLIT_SENTENCES}")
print(f"SAMPLE_RANDOM_SEED: {SAMPLE_RANDOM_SEED}")
print(f"INFO_INTERVAL: {INFO_INTERVAL}")


veld_data_yaml = {
    "x-veld": {
        "data": {
            "description": OUT_DATA_DESCRIPTION,
            "topics": "NLP",
            "contents": [
                "training data",
                "raw text",
            ],
            "file_type": "txt",
            "additional": {
                "data size": None,
            }
        }
    }
}
nlp = spacy.load("de_core_news_lg")


def get_interval_index_list(l, n):
    step = int(round(len(l) / n))
    interval_index_list = []
    for i in range(1, n + 1):
        if i < n:
            interval_index_list.append(step * i - 1)
        else:
            interval_index_list.append(len(l) - 1)
    return interval_index_list


def get_file_list_list():
    print("creating list of lists of files to process per CPU core", flush=True)
    file_list_all = [IN_JSON_FOLDER_PATH + f for f in os.listdir(IN_JSON_FOLDER_PATH)]
    file_list_all = sorted(file_list_all)
    if  SAMPLE_SIZE_PERCENTAGE != 100:
        if SAMPLE_RANDOM_SEED is not None:
            random.seed(SAMPLE_RANDOM_SEED)
        random.shuffle(file_list_all)
        sample_end_index = int((len(file_list_all) / 100) * SAMPLE_SIZE_PERCENTAGE)
        print(f"reduced file list from {len(file_list_all)} files to sample size of {sample_end_index - 1}")
        file_list_all = file_list_all[:sample_end_index]
    file_list_list = []
    file_list_tmp = []
    interval_index_list = get_interval_index_list(file_list_all, CPU_COUNT)
    for i, f in enumerate(file_list_all):
        file_list_tmp.append(f)
        if i in interval_index_list:
            file_list_list.append(file_list_tmp)
            file_list_tmp = []
    print(f"done. number of all individual files: {len(file_list_all)}, split across {CPU_COUNT}"\
        " subsets for multiprocessing.", flush=True)
    return file_list_list


def run_multi_process(file_list_list, tmp_file_list):

    def transform_files_process(tmp_file_path, p_id, file_list):
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        print(f"process {p_id}: start", flush=True)
        with open(tmp_file_path, "a") as f_out:
            interval_index_list = get_interval_index_list(file_list, INFO_INTERVAL)
            for i, file_path_in in enumerate(file_list):
                with open(file_path_in, "r") as f_in:
                    data = json.load(f_in)
                    text = data["text"].replace("\n", " ")
                    if SET_SPLIT_SENTENCES:
                        doc = nlp(text)
                        for sent in doc.sents:
                            f_out.write(sent.text + "\n")
                    else:
                        f_out.write(text + "\n")
                if i in interval_index_list:
                    print(f"process {p_id}: done with {i + 1} files, out of {len(file_list)}", \
                        flush=True)
        print(f"process {p_id}: done", flush=True)

    process_list = []
    for i, tmp_file_path in enumerate(tmp_file_list):
        process_list.append(Process(target=transform_files_process, args=(tmp_file_path, i, \
            file_list_list[i])))
    for process in process_list:
        process.start()
    for process in process_list:
        process.join()


def join_tmp_files(tmp_file_list):
    print("joining tmp files into one.", flush=True)
    with open(OUT_TXT_PATH, "w") as f_out:
        for tmp_file_path in tmp_file_list:
            with open(tmp_file_path, "r") as f_in:
                f_out.write(f_in.read())
    result = subprocess.run(["du", "-sh", OUT_TXT_PATH], capture_output=True, text=True)
    data_size = result.stdout.split()[0]
    veld_data_yaml["x-veld"]["data"]["additional"]["data size"] = data_size
    print(f"done. Size of data: {data_size}", flush=True)
    with open(OUT_VELD_DATA_YAML_PATH, "w") as f:
        yaml.dump(veld_data_yaml, f, sort_keys=False)


def main():
    file_list_list = get_file_list_list()
    tmp_file_list = [f"{TMP_FILE_FOLDER}/{i}.txt" for i in range(int(CPU_COUNT))]
    run_multi_process(file_list_list, tmp_file_list)
    join_tmp_files(tmp_file_list)


if __name__ == "__main__":
    main()

