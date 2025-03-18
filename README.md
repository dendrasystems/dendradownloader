# Dendra STAC Toolbox

ArcGIS toolbox to enable bulk downloads from the Dendra STAC API.

## Installation

1. Download the latest code from the releases page.
2. Extract the downloaded files
3. Either Click and drag the `dendra_downloader.pyt` file into the toolbox panel **OR** select toolboxes > add toolbox and load up the toolbox file

## Configuration

Configuration is handled using a .ini file.
The ini group name is the connection name.

| Option              | Required | Default | Description                                                                                                       |
|---------------------|----------|---------|-------------------------------------------------------------------------------------------------------------------|
| auth_token          | `true`   | -       | Your token to authenticate with the API                                                                           |
| catalogue_url      | `true`   | -       | The URL for a STAC catalogue urls                                                              |
| data_dir            | `true`   | -       | The output directory to put the downloaded files. This will be created, if it doesn't exist                       |
| redownload          | `false`  | false   | If `false`, files that have already been downloaded will be skipped                                               |
| add_to_active_map   | `false`  | false   | If there is an active map, data will be added to the map once it has been downloaded. Only works for tif sources. |


Example config.ini
```
[dendra]
auth_token: <auth_token>
catalogue_url: <stac_catalogue_url_1>
data_dir: <output_dir>
redownload: false
add_to_active_map: false
```

## Usage

1. Open up the toolbox
2. Add your config file using the file explorer
3. Select your host from the configuration file. This will parse the catalogue, looking for a collections and search endpoint.
4. Select from the available collection filters and hit execute.

This will begin the download process for all the downloadable resources. Files are organised by their parent collection, in the case of the Dendra catalogue, 
this means the outputs are organised by AOI. 

## Testing

Install the test requirements:

```shell
poetry install --with dev
```

Run the unit tests with:

``` shell
pytest tests/
```

Or test manually by running the script from the command line to show collection ids:

``` shell
./dendra_downloader.pyt show-collection-ids --config-path=/path/to/config.ini--host=develop
```

Or download files:

``` shell
./dendra_downloader.pyt download-files --config-path=/path/to/config.ini --host=develop --collection-ids 564, 581
```

You can also check your settings:

``` shell
./dendra_downloader.pyt show-settings --config-path=/path/to/config.ini --host=develop
```
