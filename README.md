# veld_code__wikipedia_nlp_preprocessing

This repo contains [code velds](https://zenodo.org/records/13322913) encapsulating downloading and
extracting wikipedia dumps from its official source.

## requirements

- git
- docker compose (note: older docker compose versions require running `docker-compose` instead of 
  `docker compose`)

## how to use

A code veld may be integrated into a chain veld, or used directly by adapting the configuration 
within its yaml file and using the template folders provided in this repo. Open the respective veld 
yaml file for more information.

**[./veld_download_and_extract.yaml](./veld_download_and_extract.yaml)**

Downloads and extracts an entire wikipedia dump into json files.

```
docker compose -f veld_download_and_extract.yaml up
```

**[./veld_transform_wiki_json_to_txt.yaml](./veld_transform_wiki_json_to_txt.yaml)**

Transforms the wikipedia json files into one single txt

```
docker compose -f veld_transform_wiki_json_to_txt.yaml up
```

