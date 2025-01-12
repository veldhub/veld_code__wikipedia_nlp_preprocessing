# veld_code__wikipedia_nlp_preprocessing

This repo contains code velds that help in downloading and extracting wikipedia dumps from its
official source.

## requirements

- git
- docker compose

## how to use

The following code velds may either be integrated into chain velds or used on their own by
adapting their respective veld yaml files. More information on each can be found within the veld
yaml files.

- [./veld_download_and_extract.yaml](./veld_download_and_extract.yaml) : downloads and extracts an 
entire wikipedia dump into json files.

[./veld_transform_wiki_json_to_txt.yaml](./veld_transform_wiki_json_to_txt.yaml) : transforms the 
wikipedia json files into one single txt

