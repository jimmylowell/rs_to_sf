# RS to SF
Edit config file and run!

- Gets Table DDL from redshift
- Converts redshift SQL to Snowflake
    - Does not include default values
    - DOes not include serial/sequence creation
- Runs table DDL in Snowflake DB (Make sure schema is created first)
- Unloads table data from RS into S3 bucket
- Copy cmd to load S3 files into SF table