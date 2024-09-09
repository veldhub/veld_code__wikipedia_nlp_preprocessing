import json
import os
from multiprocessing import Process

import spacy


IN_FILE_FOLDER_PATH = "/veld/input/"
OUT_FILE_PATH = "/veld/output/" + os.getenv("out_txt_file")
CPU_COUNT = os.cpu_count() - 2
INFO_INTERVAL = 100
TMP_FILE_FOLDER = "/tmp"


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
    file_list_all = [IN_FILE_FOLDER_PATH + f for f in os.listdir(IN_FILE_FOLDER_PATH)]
    file_list_list = []
    file_list_tmp = []
    interval_index_list = get_interval_index_list(file_list_all, CPU_COUNT)
    for i, f in enumerate(file_list_all):
        file_list_tmp.append(f)
        if i in interval_index_list:
            file_list_list.append(file_list_tmp)
            file_list_tmp = []
    print(f"done. number of files to processed: {len(file_list_all)}, split across {CPU_COUNT} cpu cores.", flush=True)
    return file_list_list


def run_multi_process(file_list_list, tmp_file_list):

    def transform_files_process(tmp_file_path, p_id, file_list):
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        print(f"process {p_id} start", flush=True)
        with open(tmp_file_path, "a") as f_out:
            interval_index_list = get_interval_index_list(file_list, INFO_INTERVAL)
            for i, file_path_in in enumerate(file_list):
                with open(file_path_in, "r") as f_in:
                    data = json.load(f_in)
                    text = data["text"].replace("\n", " ")
                    doc = nlp(text)
                    for sent in doc.sents:
                        f_out.write(sent.text + "\n")
                if i in interval_index_list:
                    print(f"process: {p_id}, done with {i + 1} files, out of {len(file_list)}", flush=True)
        print(f"process {p_id} done", flush=True)

    process_list = []
    for i, tmp_file_path in enumerate(tmp_file_list):
        process_list.append(Process(target=transform_files_process, args=(tmp_file_path, i, file_list_list[i])))
    for process in process_list:
        process.start()
    for process in process_list:
        process.join()


def join_tmp_files(tmp_file_list):
    print("joining tmp files into one.", flush=True)
    with open(OUT_FILE_PATH, "w") as f_out:
        for tmp_file_path in tmp_file_list:
            with open(tmp_file_path, "r") as f_in:
                f_out.write(f_in.read())
    print("done", flush=True)


def main():
    file_list_list = get_file_list_list()
    tmp_file_list = [f"{TMP_FILE_FOLDER}/{i}.txt" for i in range(int(CPU_COUNT))]
    run_multi_process(file_list_list, tmp_file_list)
    join_tmp_files(tmp_file_list)


if __name__ == "__main__":
    main()

