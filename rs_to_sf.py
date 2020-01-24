import psycopg2
import snowflake.connector
from datetime import datetime
import yaml

# from rs_to_sf_ddl import RS_TO_SF_DDL

with open("rs_to_sf_ddl.sql") as rs_to_sf_ddl:
    RS_TO_SF_DDL = rs_to_sf_ddl.read()
    rs_to_sf_ddl.close()

# Get all config properties
with open("config.yml", 'r') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

    # Redshift Config
    RS = cfg['redshift']
    RS_DB_NAME = RS['database']
    RS_HOST = RS['host']
    RS_USERNAME = RS['username']
    RS_PASS = RS['password']
    AWS_KEY_ID = RS['aws_key_id']
    AWS_SECRET_KEY = RS['aws_secret_key']
    AWS_ARN = RS['aws_arn']
    S3_BUCKET = RS['s3_bucket']

    # Snowflake Config
    SF = cfg['snowflake']
    SF_DB_NAME = SF['database']
    SF_ACCOUNT = SF['account']
    SF_USERNAME = SF['username']
    SF_PASS = SF['password']

    TABLE_LIST = cfg['table_list']

UNLOAD_CMD = r"""
UNLOAD ('SELECT * FROM {schema_name}.{table_name}')
to 's3://{s3_bucket}/{schema_name}/{table_name}/' 
iam_role '{aws_arn}'
gzip
maxfilesize 500mb
delimiter '\001'
null '\\N'
allowoverwrite
--addquotes
escape
;   
"""

COPY_CMD = r"""
COPY into {database_name}.{schema_name}.{table_name}
from s3://{s3_bucket}/{schema_name}/{table_name}/
credentials=(aws_key_id='{aws_key_id}', aws_secret_key='{aws_secret_key}')
file_format = (
  type = csv
  field_delimiter = '\001'
  TRIM_SPACE = TRUE
  null_if = ('\\N')
  --FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
  ESCAPE = '\134'
)
--ON_ERROR = SKIP_FILE

"""


def run_rs_cmd(cmd):
    con = psycopg2.connect(
        dbname=RS_DB_NAME,
        host=RS_HOST,
        port=5439,
        user=RS_USERNAME,
        password=RS_PASS)
    cur = con.cursor()
    cur.execute(cmd)
    try:
        return cur.fetchall()
    except:
        return


def run_sf_cmd(cmd):
    con = snowflake.connector.connect(
        user=SF_USERNAME,
        password=SF_PASS,
        account=SF_ACCOUNT,
    )
    print("USING DATABASE: " + SF_DB_NAME)
    con.cursor().execute('USE DATABASE ' + SF_DB_NAME)
    try:
        con.cursor().execute(cmd)
    except snowflake.connector.errors.ProgrammingError as e:
        print('ERROR: ' + e.msg)


# def get_rs_default_values(schema_name):
#     rs_default_values_cmd = RS_TO_SF_DDL.format(
#         schema_name=schema_name,
#         additional_filter="AND ddl LIKE '% DEFAULT %'"
#     )
#     rs_default_values = run_rs_cmd(rs_default_values_cmd)
#     rs_defaults_to_sf(rs_default_values)


def create_sf_table(schema_name, table_name):
    sf_table_ddl = get_sf_ddl_from_rs(schema_name, table_name)
    run_sf_cmd(sf_table_ddl)


def get_sf_ddl_from_rs(schema_name, table_name):
    rs_to_sf_cmd = RS_TO_SF_DDL.format(
        table_name=table_name,
        schema_name=schema_name
    )
    print(rs_to_sf_cmd)
    # sf_ddl_list = run_rs_cmd(rs_to_sf_cmd)
    sf_table_ddl = ""
    # for record in sf_ddl_list:
    #     ddl_line = record[0]
    #     sf_table_ddl += (ddl_line + "\n")
    # print(sf_table_ddl)
    return sf_table_ddl


def rs_unload(schema_name, table_name):
    unload_table_cmd = UNLOAD_CMD.format(
        schema_name=schema_name,
        table_name=table_name,
        aws_arn=AWS_ARN,
        s3_bucket=S3_BUCKET

    )
    print(unload_table_cmd)
    # try:
    #     run_rs_cmd(unload_table_cmd)
    # except psycopg2.InternalError as e:
    #     print("ERROR: " + e.pgerror)


def sf_copy_to(schema_name, table_name):
    copy_table_cmd = COPY_CMD.format(
        database_name=SF_DB_NAME,
        schema_name=schema_name,
        table_name=table_name,
        aws_key_id=AWS_KEY_ID,
        aws_secret_key=AWS_SECRET_KEY,
        s3_bucket=S3_BUCKET
    )
    print(copy_table_cmd)
    # run_sf_cmd(copy_table_cmd)


if __name__ == "__main__":
    table_count = 0
    load_start_time = datetime.now()

    for table in TABLE_LIST:
        # TCOB
        table = table.lower()
        schema_name, table_name = table.split(".")
        table_count += 1
        table_start_time = datetime.now()
        print(schema_name, table_name)

        # Create redshift table in snowflake
        create_sf_table(schema_name, table_name)

        # Do the Redshift unload first
        # rs_unload(schema_name, table_name)
        rs_unload_end_time = datetime.now()
        rs_unload_elapsed_time = rs_unload_end_time - table_start_time

        # Then copy to command
        # sf_copy_to(schema_name, table_name)
        sf_copy_end_time = datetime.now()
        sf_copy_elapsed_time = sf_copy_end_time - rs_unload_end_time

        # Print some stuff
        table_total_elapsed_time = datetime.now() - table_start_time
        print(table_name, datetime.now(), rs_unload_elapsed_time, sf_copy_elapsed_time, table_total_elapsed_time)

    # Print some stuff
    load_elapsed_time = datetime.now() - load_start_time
    print(table_count, load_elapsed_time)
