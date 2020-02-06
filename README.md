# RS to SF
Requires [psycopg2], [pyyaml], [datetime], and [snowflake-connector]

[psycopg2]: https://pypi.org/project/psycopg2/
[snowflake-connector]: https://docs.snowflake.net/manuals/user-guide/python-connector.html
[pyyaml]: https://pypi.org/project/PyYAML/
[datetime]: https://pypi.org/project/datetime3/

```shell script
pip3 install psycopg2 pyyaml datetime
pip3 install --upgrade snowflake-connector-python
```
Edit config file, rename to config.yml and run!
```shell script
python3 rs_to_sf.py
```
- Gets Table DDL from Redshift - many thanks to: 

https://github.com/awslabs/amazon-redshift-utils/blob/master/src/AdminViews/v_generate_tbl_ddl.sql

- Converts Redshift table ddl / data types to Snowflake
    - Does not include default values
    - Does not include serial/sequence creation
        - There are some comments in SQL file on how to do this.
        - You'll want to create sequences, 
        then get table DDL that includes default values for those sequences.
        - I didn't run into any other scenarios/issues, 
        but I"m sure they are out there...
- CREATE table DDL in Snowflake DB (Make sure schema is created first)
- UNLOAD table data from RS into S3 bucket
- COPY cmd to load S3 files into SF table

To Do:
- [ ] argparse for config file location
- [ ] build in sequence generation
- [ ] proper logging
- [ ] multithreading/asyncio?