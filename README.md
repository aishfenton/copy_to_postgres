# Copy To Postgres

Alpha build, use at your own risk.

## Install

TODO 
- Add yum,deb, and brew installers


## Usage

```
Usage: 
    copy_to_postgres -m mapping -t table --db-url url [--type JSON|AVRO|CSV] [--db-user user] [--db-pass pass] [--version] input-file

Example:

>copy_to_postgres -t mytable --type AVRO --db-url localhost/test -m ' \
    source_field -> dest_field, \
    "string literal" -> dest_description, \
    2.23 -> dest_field_2, \ 
    1002 -> dest_field_3, \ 
  myfile.avro
```

## Features 

- Can read Avro, Json, Csv and Tsv files.

- Uses Postgres copy command for fast appending to tables

- Provides a simple mapping language for wiring what fields go where within the database
 
## Cookbook

TODO. Explain various strategies around fast appending into tables
