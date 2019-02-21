
# tap-toast

Tap for [Toast Data](https://pos.toasttab.com/).

## Requirements

- pip3
- python 3.5+
- mkvirtualenv

## Installation

In the directory:

```
$ mkvirtualenv -p python3 tap-toast
$ pip3 install -e .
```

## Usage

### Create config file

You can get all of the below from talking to a sales representative at Toast (totally obnoxious, I know).

```
{
  "client_id": "***",
  "client_secret": "***",
  "location_guid": "***",
  "start_date": "2018-11-12T11:00:30+00:00"
}
```

Toast is one of those companies where the API can only be accessed by talking to their sales team and signing an sales contract. Once the contract is in place, then their sales team will set up your account and email you the credentials necessary. **You will not be able to generate these keys on your own in the development portal.**

Here is an example of the credentials that the Toast sales team will provide you:

```
client ID: your-client-id
client secret: *FHHCsdqpme!@*$#
location GUID: 93djm422-bdu4-mpt3-148s-34ctcm8mp4jf
```

The `start_date` is just the date you want the sync to begin. You can select this yourself.

```
start_date: 2018-11-12T11:00:30+00:00
```

### Discovery mode

This command returns a JSON that describes the schema of each table.

```
$ tap-toast --config config.json --discover
```

To save this to `catalog.json`:

```
$ tap-toast --config config.json --discover > catalog.json
```

### Field selection

You can tell the tap to extract specific fields by editing `catalog.json` to make selections. Note the top-level `selected` attribute, as well as the `selected` attribute nested under each property.

```
{
  "selected": "true",
  "properties": {
    "likes_getting_petted": {
      "selected": "true",
      "inclusion": "available",
      "type": [
        "null",
        "boolean"
      ]
    },
    "name": {
      "selected": "true",
      "maxLength": 255,
      "inclusion": "available",
      "type": [
        "null",
        "string"
      ]
    },
    "id": {
      "selected": "true",
      "minimum": -2147483648,
      "inclusion": "automatic",
      "maximum": 2147483647,
      "type": [
        "null",
        "integer"
      ]
    }
  },
  "type": "object"
}
```

### Sync Mode

With an annotated `catalog.json`, the tap can be invoked in sync mode:

```
$ tap-toast --config config.json --catalog catalog.json
```

Messages are written to standard output following the Singer specification. The resultant stream of JSON data can be consumed by a Singer target.


## Replication Methods and State File

### Incremental

The streams that are incremental are:

- orders

### Full Table

None currently.

Copyright &copy; 2018 Stitch
