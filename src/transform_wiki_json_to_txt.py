import json
import math
import os
import random
import subprocess
from datetime import datetime
from multiprocessing import Process

import spacy
import yaml


IN_JSON_FOLDER_PATH = "/veld/input/"
OUT_TXT_PATH = "/veld/output/" + os.getenv("out_txt_file")
OUT_LOG_PATH = "/veld/output/log.txt"
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
BUFFER_SEGMENTS = int(os.getenv("buffer_segments"))
SET_SPLIT_SENTENCES = os.getenv("set_split_sentences")
if SET_SPLIT_SENTENCES.lower() == "true":
    SET_SPLIT_SENTENCES = True
else:
    SET_SPLIT_SENTENCES = False
TMP_FILE_FOLDER = "/tmp"
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


def print_and_log(msg):
    print(msg, flush=True)
    with open(OUT_LOG_PATH, "a") as out:
        out.write(msg + "\n")


def get_file_list():
    print_and_log("creating list of lists of files to process")
    file_list = [IN_JSON_FOLDER_PATH + f for f in os.listdir(IN_JSON_FOLDER_PATH)]
    file_list = sorted(file_list)
    if  SAMPLE_SIZE_PERCENTAGE != 100:
        if SAMPLE_RANDOM_SEED is not None:
            random.seed(SAMPLE_RANDOM_SEED)
        random.shuffle(file_list)
        sample_end_index = int((len(file_list) / 100) * SAMPLE_SIZE_PERCENTAGE)
        print_and_log(f"reduced file list from {len(file_list)} files to sample size of {sample_end_index - 1}")
        file_list = file_list[:sample_end_index]
    return file_list


def single_process(p_id, individual_list):
    print_and_log(f"process {p_id}: start")
    out_tmp_file_path = f"{TMP_FILE_FOLDER}/{p_id}.txt"
    if os.path.exists(out_tmp_file_path):
        os.remove(out_tmp_file_path)
    buffer_segment_step = math.ceil(len(individual_list) / BUFFER_SEGMENTS)
    buffer_out_str = ""
    with open(out_tmp_file_path, "a") as f_out:
        for i, in_file_path in enumerate(individual_list):
            with open(in_file_path, "r") as f_in:
                data = json.load(f_in)
                text = data["text"].replace("\n", " ")
                if text != "":
                    if SET_SPLIT_SENTENCES:
                        doc = nlp(text)
                        for sent in doc.sents:
                            buffer_out_str += sent.text + "\n"
                    else:
                        buffer_out_str += text + "\n"
                    if i != 0 and (i % buffer_segment_step == 0 or i == len(individual_list) -1):
                        f_out.write(buffer_out_str)
                        buffer_out_str = ""
                        print_and_log(
                            f"process {p_id}: done with {i + 1} files, out of {len(individual_list)}"
                        )
    print_and_log(f"process {p_id}: done")


def multi_process(cpu_cores, global_list, single_process_function):

    def get_segment_index_list(list_len, num_segment):
        segment_index_list = []
        step = list_len // num_segment
        i_start = 0
        for i_segment in range(1, num_segment + 1):
            if i_segment < num_segment:
                i_end = i_segment * step
                segment_index_list.append((i_start, i_end))
                i_start = i_end
            else:
                segment_index_list.append((i_start, list_len))
        return segment_index_list

    def multi_process_main():
        segment_index_list = get_segment_index_list(len(global_list), cpu_cores)
        process_list = []
        for p_id, i_start_end_tuple in enumerate(segment_index_list):
            print_and_log(
                f"process id {p_id}: assigned to indices from {i_start_end_tuple[0]} to "
                f"{i_start_end_tuple[1] - 1}"
            )
            sub_list = global_list[i_start_end_tuple[0]:i_start_end_tuple[1]]
            process_list.append(Process(target=single_process_function, args=(p_id, sub_list)))
        for process in process_list:
            process.start()
        for process in process_list:
            process.join()

    multi_process_main()


def join_tmp_files():
    print_and_log("joining tmp files into one.")
    with open(OUT_TXT_PATH, "w") as f_out:
        for tmp_file_path in [TMP_FILE_FOLDER + "/" + f for f in os.listdir(TMP_FILE_FOLDER)]:
            with open(tmp_file_path, "r") as f_in:
                f_out.write(f_in.read())
    result = subprocess.run(["du", "-sh", OUT_TXT_PATH], capture_output=True, text=True)
    data_size = result.stdout.split()[0]
    veld_data_yaml["x-veld"]["data"]["additional"]["data size"] = data_size
    print_and_log(f"done. Size of data: {data_size}")
    with open(OUT_VELD_DATA_YAML_PATH, "w") as f:
        yaml.dump(veld_data_yaml, f, sort_keys=False)


def main():
    try:
        os.remove(OUT_LOG_PATH)
    except:
        pass
    print_and_log(f"starting at: {datetime.now()}")
    print_and_log(f"OUT_TXT_PATH: {OUT_TXT_PATH}")
    print_and_log(f"CPU_COUNT: {CPU_COUNT}")
    print_and_log(f"SAMPLE_SIZE_PERCENTAGE: {SAMPLE_SIZE_PERCENTAGE}")
    print_and_log(f"SET_SPLIT_SENTENCES: {SET_SPLIT_SENTENCES}")
    print_and_log(f"SAMPLE_RANDOM_SEED: {SAMPLE_RANDOM_SEED}")
    print_and_log(f"BUFFER_SEGMENTS: {BUFFER_SEGMENTS}")

    in_file_list = get_file_list()
    multi_process(
        cpu_cores=CPU_COUNT, 
        global_list=in_file_list, 
        single_process_function=single_process
    )
    join_tmp_files()
    print_and_log(f"done at: {datetime.now()}")


if __name__ == "__main__":
    main()

