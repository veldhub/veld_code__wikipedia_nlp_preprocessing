#!/bin/bash

mkdir /tmp/compressed/ 2> /dev/null
mkdir /tmp/raw/ 2> /dev/null
tmp_compressed_path=/tmp/compressed/dump.bz2 
tmp_extracted_path=/tmp/raw/

if ! [[ -e "$tmp_compressed_path" ]]; then
  echo "downloading"
  wget "$wikipedia_dump_url" -O "$tmp_compressed_path"
else
  echo "using pre-existing local dump"
fi

if [[ -z "$(ls ${tmp_extracted_path})" ]]; then
  echo "extracting"
  python3 -m wikiextractor.WikiExtractor --json "$tmp_compressed_path" -o "$tmp_extracted_path"
else
  echo "using pre-existing extracted data"
fi

export tmp_extracted_path
python3 /veld/code/download_and_extract.py

