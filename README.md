# Dendra STAC Toolbox

ArcGIS toolbox to enable bulk downloads from the Dendra STAC API.

## Installation

1. Download the latest code from the releases page.
2. Extract the downloaded files
3. Either Click and drack the `dendra_downloader.pyt` file into the toolbox panel **OR** select toolboxes > add toolbox and load up the toolbox file

## Configuration

Configuration is handled using a .ini file.
The ini group name is the connection name.

| Option | Required | Default | Description |
| ------ | -------- | ------- | ----------- |
| stac_url | `true` | - | The URL of the STAC catalog to load |
| auth_token | `true` | - | Your token to authenticate with the API |
| data_dir | `true` | - | The output directory to put the downloaded files. This will be created, if it doesn't exist |
| cache_duration_mins | `true` | - | The STAC catalog response is cached to disk to reduce read time |
| redownload | `true` | - | If `false`, files that have already been downloaded will be skipped |
| add_to_active_map | `true` | - | If there is an active map, data will be added to the map once it has been downloaded. Only works for tif sources. |

Example config.ini
```
[dendra]
stac_url: <stac_catalog_url>
auth_token: <auth_token>
data_dir: <output_dir>
cache_duration_mins: 10
redownload: false
add_to_active_map: false
```

## Usage

1. Open up the toolbox
2. Add your config file using the file explorer
3. Select your host from the configuration file. This will parse the catalog, looking for a collections and search endpoint.
4. Optionally select from the available collection filters and hit execute.

This will begin the download process for all the downloadable resources. Files are organised by their parent collection, in the case of the Dendra catalog, 
this means the outputs are organised by AOI.
