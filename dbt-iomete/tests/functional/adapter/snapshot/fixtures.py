# Spark versions of the fixtures in dbt.tests.adapter.simple_snapshot. The upstream ones are
# Postgres SQL (VARCHAR/TEXT columns, ::casts, string literals into timestamps, several
# statements per string), so each statement here runs on its own against an Iceberg table.

_rows = [
    (1, "Judith", "Kennedy", "(not provided)", "Female", "54.60.24.128", "2015-12-24 12:19:28"),
    (2, "Arthur", "Kelly", "(not provided)", "Male", "62.56.24.215", "2015-10-28 16:22:15"),
    (3, "Rachel", "Moreno", "rmoreno2@msu.edu", "Female", "31.222.249.23", "2016-04-05 02:05:30"),
    (4, "Ralph", "Turner", "rturner3@hp.com", "Male", "157.83.76.114", "2016-08-08 00:06:51"),
    (5, "Laura", "Gonzales", "lgonzales4@howstuffworks.com", "Female", "30.54.105.168", "2016-09-01 08:25:38"),
    (6, "Katherine", "Lopez", "klopez5@yahoo.co.jp", "Female", "169.138.46.89", "2016-08-30 18:52:11"),
    (7, "Jeremy", "Hamilton", "jhamilton6@mozilla.org", "Male", "231.189.13.133", "2016-07-17 02:09:46"),
    (8, "Heather", "Rose", "hrose7@goodreads.com", "Female", "87.165.201.65", "2015-12-29 22:03:56"),
    (9, "Gregory", "Kelly", "gkelly8@trellian.com", "Male", "154.209.99.7", "2016-03-24 21:18:16"),
    (10, "Rachel", "Lopez", "rlopez9@themeforest.net", "Female", "237.165.82.71", "2016-08-20 15:44:49"),
    (11, "Donna", "Welch", "dwelcha@shutterfly.com", "Female", "103.33.110.138", "2016-02-27 01:41:48"),
    (12, "Russell", "Lawrence", "rlawrenceb@qq.com", "Male", "189.115.73.4", "2016-06-11 03:07:09"),
    (13, "Michelle", "Montgomery", "mmontgomeryc@scientificamerican.com", "Female", "243.220.95.82", "2016-06-18 16:27:19"),
    (14, "Walter", "Castillo", "wcastillod@pagesperso-orange.fr", "Male", "71.159.238.196", "2016-10-06 01:55:44"),
    (15, "Robin", "Mills", "rmillse@vkontakte.ru", "Female", "172.190.5.50", "2016-10-31 11:41:21"),
    (16, "Raymond", "Holmes", "rholmesf@usgs.gov", "Male", "148.153.166.95", "2016-10-03 08:16:38"),
    (17, "Gary", "Bishop", "gbishopg@plala.or.jp", "Male", "161.108.182.13", "2016-08-29 19:35:20"),
    (18, "Anna", "Riley", "arileyh@nasa.gov", "Female", "253.31.108.22", "2015-12-11 04:34:27"),
    (19, "Sarah", "Knight", "sknighti@foxnews.com", "Female", "222.220.3.177", "2016-09-26 00:49:06"),
    (20, "Phyllis", "Fox", None, "Female", "163.191.232.95", "2016-08-21 10:35:19"),
]


def _literal(value):
    return "null" if value is None else f"'{value}'"


def _seed_values(multi_key=False):
    values = []
    for id_, first, last, email, gender, ip, updated_at in _rows:
        keys = f"{id_}, {id_ * 100}" if multi_key else f"{id_}"
        values.append(
            f"({keys}, '{first}', '{last}', {_literal(email)}, '{gender}', '{ip}', timestamp'{updated_at}')"
        )
    return ",\n".join(values)


_person_columns = """
    first_name string,
    last_name string,
    email string,
    gender string,
    ip_address string,
    updated_at timestamp"""

_person_select = "first_name, last_name, email, gender, ip_address, updated_at"

# The snapshot's unique_key expression. The cast keeps it valid with ANSI mode on.
unique_key_expr = "cast(id as string) || '-' || first_name"

# What iomete__snapshot_hash_arguments produces for the timestamp strategy: md5 of the
# unique key(s) and updated_at joined with '|'
_scd_id = f"md5({unique_key_expr} || '|' || cast(updated_at as string))"
_multi_key_scd_id = "md5(cast(id1 as string) || '|' || cast(id2 as string) || '|' || cast(updated_at as string))"


snapshot_actual_sql = """
{% snapshot snapshot_actual %}
    {{ config(unique_key="cast(id as string) || '-' || first_name") }}
    select * from {{ target.database }}.{{ target.schema }}.seed
{% endsnapshot %}
"""

ref_snapshot_sql = """
select * from {{ ref('snapshot_actual') }}
"""

# A model named like the seed table, so a yaml-only snapshot can point at it with ref('seed').
# It is never run; ref('seed') resolves to the table the test creates.
model_seed_sql = """
select * from {{ target.database }}.{{ target.schema }}.seed
"""


# --- snapshot_meta_column_names and dbt_valid_to_current ----------------------------------

_meta_column_names_yml = """
      snapshot_meta_column_names:
          dbt_valid_to: TEST_VALID_TO
          dbt_valid_from: test_valid_from
          dbt_scd_id: test_scd_id
          dbt_updated_at: test_updated_at
"""

snapshots_yml = (
    """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at"""
    + _meta_column_names_yml
)

snapshots_no_column_names_yml = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
"""

valid_to_current = "date('2099-12-31')"

snapshots_valid_to_current_yml = (
    """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
      dbt_valid_to_current: "date('2099-12-31')\""""
    + _meta_column_names_yml
)

create_seed_sql = f"""
create table {{schema}}.seed (
    id int,{_person_columns}
) using iceberg
"""

seed_insert_sql = f"""
insert into {{schema}}.seed values
{_seed_values()}
"""

create_snapshot_expected_sql = f"""
create table {{schema}}.snapshot_expected (
    id int,{_person_columns},
    test_valid_from timestamp,
    TEST_VALID_TO timestamp,
    test_scd_id string,
    test_updated_at timestamp
) using iceberg
"""


def _expected_rows_sql(valid_to, where=""):
    return f"""
insert into {{schema}}.snapshot_expected
select
    id, {_person_select},
    updated_at as test_valid_from,
    {valid_to} as TEST_VALID_TO,
    {_scd_id} as test_scd_id,
    updated_at as test_updated_at
from {{schema}}.seed
{where}
"""


populate_snapshot_expected_sql = _expected_rows_sql("cast(null as timestamp)")
populate_snapshot_expected_valid_to_current_sql = _expected_rows_sql(f"cast({valid_to_current} as timestamp)")

# Change rows 10 - 20 in the source, and close their current versions in the expected table
invalidate_sql_statements = [
    """
update {schema}.seed set
    updated_at = updated_at + interval 1 hour,
    email = case when id = 20 then 'pfoxj@creativecommons.org' else 'new_' || email end
where id >= 10 and id <= 20
""",
    """
update {schema}.snapshot_expected set
    TEST_VALID_TO = updated_at + interval 1 hour
where id >= 10 and id <= 20
""",
]

# The new versions of rows 10 - 20
update_sql = _expected_rows_sql("cast(null as timestamp)", "where id >= 10 and id <= 20")
update_with_current_sql = _expected_rows_sql(
    f"cast({valid_to_current} as timestamp)", "where id >= 10 and id <= 20"
)


# --- list unique_key ----------------------------------------------------------------------

snapshots_multi_key_yml = (
    """
snapshots:
  - name: snapshot_actual
    relation: "ref('seed')"
    config:
      strategy: timestamp
      updated_at: updated_at
      unique_key:
        - id1
        - id2"""
    + _meta_column_names_yml
)

create_multi_key_seed_sql = f"""
create table {{schema}}.seed (
    id1 int,
    id2 int,{_person_columns}
) using iceberg
"""

seed_multi_key_insert_sql = f"""
insert into {{schema}}.seed values
{_seed_values(multi_key=True)}
"""

create_multi_key_snapshot_expected_sql = f"""
create table {{schema}}.snapshot_expected (
    id1 int,
    id2 int,{_person_columns},
    test_valid_from timestamp,
    TEST_VALID_TO timestamp,
    test_scd_id string,
    test_updated_at timestamp
) using iceberg
"""


def _expected_multi_key_rows_sql(where=""):
    return f"""
insert into {{schema}}.snapshot_expected
select
    id1, id2, {_person_select},
    updated_at as test_valid_from,
    cast(null as timestamp) as TEST_VALID_TO,
    {_multi_key_scd_id} as test_scd_id,
    updated_at as test_updated_at
from {{schema}}.seed
{where}
"""


populate_multi_key_snapshot_expected_sql = _expected_multi_key_rows_sql()

invalidate_multi_key_sql_statements = [
    """
update {schema}.seed set
    updated_at = updated_at + interval 1 hour,
    email = case when id1 = 20 then 'pfoxj@creativecommons.org' else 'new_' || email end
where id1 >= 10 and id1 <= 20
""",
    """
update {schema}.snapshot_expected set
    TEST_VALID_TO = updated_at + interval 1 hour
where id1 >= 10 and id1 <= 20
""",
]

update_multi_key_sql = _expected_multi_key_rows_sql("where id1 >= 10 and id1 <= 20")


# --- hard_deletes: new_record -------------------------------------------------------------

def _new_record_expected_rows_sql(where=""):
    return f"""
insert into {{schema}}.snapshot_expected
select
    id, {_person_select},
    updated_at as dbt_valid_from,
    cast(null as timestamp) as dbt_valid_to,
    {_scd_id} as dbt_scd_id,
    updated_at as dbt_updated_at,
    'False' as dbt_is_deleted
from {{schema}}.seed
{where}
"""


new_record_seed_statements = [
    create_seed_sql,
    f"""
create table {{schema}}.snapshot_expected (
    id int,{_person_columns},
    dbt_valid_from timestamp,
    dbt_valid_to timestamp,
    dbt_scd_id string,
    dbt_updated_at timestamp,
    dbt_is_deleted string
) using iceberg
""",
    seed_insert_sql,
    _new_record_expected_rows_sql(),
]

new_record_invalidate_sql_statements = [
    invalidate_sql_statements[0],
    """
update {schema}.snapshot_expected set
    dbt_valid_to = updated_at + interval 1 hour
where id >= 10 and id <= 20
""",
]

new_record_update_sql = _new_record_expected_rows_sql("where id >= 10 and id <= 20")

new_record_delete_sql = """
delete from {schema}.seed where id = 1
"""

new_record_reinsert_sql = """
insert into {schema}.seed values
(1, 'Judith', 'Kennedy', '(not provided)', 'Female', '54.60.24.128', timestamp'2200-01-01 12:00:00')
"""

new_record_valid_to_current_seed_statements = [
    "create table {schema}.seed (id int, first_name string) using iceberg",
    "insert into {schema}.seed values (1, 'Judith'), (2, 'Arthur')",
]
