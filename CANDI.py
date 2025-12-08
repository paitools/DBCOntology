import os
import duckdb
import pandas as pd
from rdflib import Graph, RDF, RDFS, OWL
from collections import defaultdict


# ---------------------------------------------------
# USER CONFIGURATION
# ---------------------------------------------------

KG_Matrix = "KGM.xlsx"
DuckDB = "ontop.duckdb"
output_dir = "owl"
raw_data_path ="raw/*/*/*/*.csv"


# ---------------------------------------------------
# UTILITIES
# ---------------------------------------------------

def explode_multivalue_columns(df, delimiter=","):
    """Explodes comma-separated columns into multiple rows."""
    df = df.copy()

    multi_value_cols = [
        col for col in df.columns
        if df[col].dropna().astype(str).str.contains(delimiter).any()
    ]

    for col in multi_value_cols:
        df[col] = df[col].astype(str).str.split(delimiter)

    if multi_value_cols:
        df = df.explode(multi_value_cols).reset_index(drop=True)
        for col in multi_value_cols:
            df[col] = df[col].str.strip()

    return df


def print_preview(con, view_name, limit=10):
    """Pretty prints `SELECT * FROM view LIMIT n`."""
    print(f"\n✅ {view_name} view created:")

    result = con.execute(f"SELECT * FROM {view_name} LIMIT {limit}")
    rows = result.fetchall()
    cols = [desc[0] for desc in result.description]

    if not rows:
        print("(no rows)")
        return

    widths = [
        max(len(str(col)), max(len(str(row[i])) for row in rows))
        for i, col in enumerate(cols)
    ]

    print("  ".join(col.ljust(widths[i]) for i, col in enumerate(cols)))
    for row in rows:
        print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(cols))))


def create_view(con, name, sql):
    """Executes a CREATE VIEW and prints preview."""
    con.execute(sql)
    print_preview(con, name)


# ---------------------------------------------------
# EXPORT SHEETS
# ---------------------------------------------------

print("\n🔁 Exporting sheets to CSV...")
os.makedirs(output_dir, exist_ok=True)

sheets = pd.read_excel(KG_Matrix, sheet_name=None)

for sheet_name, df in sheets.items():
    csv_path = os.path.join(output_dir, f"{sheet_name}.csv")
    df_clean = explode_multivalue_columns(df)
    df_clean.to_csv(csv_path, index=False, sep=';')
    print(f"Sheet '{sheet_name}' exported to '{csv_path}'")


# ---------------------------------------------------
# CREATE VIEWS
# ---------------------------------------------------

con = duckdb.connect(DuckDB)

# subclasses
create_view(con, "subclasses", """
CREATE OR REPLACE VIEW subclasses AS
SELECT
    "Class" AS Class,
    "rdfs:subClassOf" AS rdfs__subClassOf
FROM read_csv_auto(
    'owl/Subclass.csv',
    DELIM=';',
    HEADER=TRUE,
    AUTO_DETECT=TRUE
)
""")

# messagelog
create_view(con, "messagelog", f"""
CREATE OR REPLACE VIEW messagelog AS
SELECT
    REPLACE(
        split_part(REPLACE(filename, '\\', '/'), '/', -1),
        '.csv', ''
    ) AS sensor,
    timestamp,
    busChannel,
    ide,
    data
FROM read_csv_auto(
    '{raw_data_path}',
    AUTO_DETECT=TRUE,
    FILENAME=TRUE
)
""")

# node
create_view(con, "node", """
CREATE OR REPLACE VIEW node AS
SELECT
    "Individual" AS Individual,
    "rdf:type" AS rdf__type
FROM read_csv_auto(
    'owl/Node.csv',
    DELIM=';',
    HEADER=TRUE,
    AUTO_DETECT=TRUE
)
""")

# message
create_view(con, "message", """
CREATE OR REPLACE VIEW message AS
SELECT
    "Individual" AS Individual,
    "rdf:type" AS rdf__type,
    "dbc:dataLength" AS dbc__dataLength,
    "dbc:encodedVia" AS dbc__encodedVia,
    "dbc:hasDecID" AS dbc__hasDecID,
    "dbc:hasSignal" AS dbc__hasSignal,
    "dbc:hasTransmitter" AS dbc__hasTransmitter,
    "sosa:isObservedBy" AS sosa__isObservedBy
FROM read_csv_auto(
    'owl/Message.csv',
    DELIM=';',
    HEADER=TRUE,
    AUTO_DETECT=TRUE
)
""")

# signal
create_view(con, "signal", """
CREATE OR REPLACE VIEW signal AS
SELECT
    "Individual" AS Individual,
    "rdf:type" AS rdf__type,
    "dbc:decodedVia" AS dbc__decodedVia,
    "dbc:hasReceiver" AS dbc__hasReceiver,
    "dbc:isPartOf" AS dbc__isPartOf,
    "qudt:hasUnit" AS qudt__hasUnit,
    "sosa:isObservedBy" AS sosa__isObservedBy
FROM read_csv_auto(
    'owl/Signal.csv',
    DELIM=';',
    HEADER=TRUE,
    AUTO_DETECT=TRUE
)
""")

# sensor
create_view(con, "sensor", """
CREATE OR REPLACE VIEW sensor AS
SELECT
    "Individual" AS Individual,
    "rdf:type" AS rdf__type,
    "sosa:isHostedBy" AS sosa__isHostedBy,
    "sosa:madeObservation" AS sosa__madeObservation,
    "sosa:observes" AS sosa__observes
FROM read_csv_auto(
    'owl/Sensor.csv',
    DELIM=';',
    HEADER=TRUE,
    AUTO_DETECT=TRUE
)
""")

# signalencoding
create_view(con, "signalencoding", """
CREATE OR REPLACE VIEW signalencoding AS
SELECT
    "Individual" AS Individual,
    "rdf:type" AS rdf__type,
    "dbc:bitLenght" AS dbc__bitLenght,
    "dbc:bitStart" AS dbc__bitStart,
    "dbc:signed" AS dbc__signed,
    "qudt:byteOrder" AS qudt__byteOrder,
    "qudt:conversionMultiplier" AS qudt__conversionMultiplier,
    "qudt:conversionOffset" AS qudt__conversionOffset,
    "qudt:maxInclusive" AS qudt__maxInclusive,
    "qudt:minInclusive" AS qudt__minInclusive
FROM read_csv_auto(
    'owl/SignalEncoding.csv',
    DELIM=';',
    HEADER=TRUE,
    AUTO_DETECT=TRUE
)
""")

# platform
create_view(con, "platform", """
CREATE OR REPLACE VIEW platform AS
SELECT
    "Individual" AS Individual,
    "rdf:type" AS rdf__type,
    "sosa:hosts" AS sosa__hosts
FROM read_csv_auto(
    'owl/Platform.csv',
    DELIM=';',
    HEADER=TRUE,
    AUTO_DETECT=TRUE
)
""")

# signallog
create_view(con, "signallog", """
CREATE OR REPLACE VIEW signallog AS
WITH joined AS (
    SELECT
        ml.timestamp,
        ml.data,
        s.Individual AS signal_individual,
        e.dbc__bitStart,
        e.dbc__bitLenght,
        CASE WHEN e.dbc__signed THEN 1 ELSE 0 END AS is_signed,
        e.qudt__byteOrder,
        e.qudt__conversionMultiplier,
        e.qudt__conversionOffset
    FROM MessageLog ml
    JOIN Message m
        ON CAST('0x' || ml.ide AS UBIGINT) = m.dbc__hasDecID
    JOIN Signal s
        ON s.dbc__isPartOf = m.Individual
    JOIN SignalEncoding e
        ON e.Individual = s.dbc__decodedVia
),

decoded AS (
    SELECT
        *,
        CASE
            WHEN qudt__byteOrder = 'LittleEndian' THEN
                CAST(
                    '0x' ||
                    substr(data,15,2) || substr(data,13,2) || substr(data,11,2) ||
                    substr(data,9,2)  || substr(data,7,2)  || substr(data,5,2) ||
                    substr(data,3,2)  || substr(data,1,2)
                AS UBIGINT)
            ELSE CAST('0x' || data AS UBIGINT)
        END AS raw_value
    FROM joined
)

SELECT
    concat(
        signal_individual, '_',
        strftime(timestamp, '%Y%m%d%H%M%S'), '_',
        row_number() OVER ()
    ) AS Individual,
    'dbc:SignalLog' AS rdf__type,
    signal_individual AS dbc__decodedFrom,

    CAST(
        (
            ((raw_value >> dbc__bitStart) &
                ((1::UBIGINT << dbc__bitLenght) - 1))
            - CASE
                WHEN is_signed = 1 AND
                    (((raw_value >> dbc__bitStart) &
                      (1::UBIGINT << (dbc__bitLenght - 1))) != 0)
                THEN (1::UBIGINT << dbc__bitLenght)
                ELSE 0
              END
        ) * qudt__conversionMultiplier
        + qudt__conversionOffset
    AS DOUBLE) AS sosa__hasSimpleResult,

    'can2_sniffer' AS sosa__madeBySensor,
    signal_individual AS sosa__observedProperty,
    timestamp AS sosa__resultTime

FROM decoded;
""")

