from datetime import date
import psycopg
from configs.config import settings

SOURCE2TABLE = {
    "yellow": settings.datamart_yellow_taxi_table,
    "green": settings.datamart_green_taxi_table,
    "fhvhv": settings.datamart_fhvhv_table
}

def partition_exists(conn, table_name):
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = %s
        );
    """
    with conn.cursor() as cur:
        cur.execute(query, (table_name,))
        return cur.fetchone()[0]


def create_partition(conn, datasource, year, month):
    parent_table = SOURCE2TABLE[datasource]
    partition_table = f"{parent_table}_{year}_{month:02d}"

    start_date = date(year, month, 1)
    if month == 12:
        end_date = date(year + 1, 1, 1)
    else:
        end_date = date(year, month + 1, 1)

    sql = f"""
    CREATE TABLE {partition_table}
    PARTITION OF {parent_table}
    FOR VALUES FROM ('{start_date}') TO ('{end_date}');
    """

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(f"✅ Created partition {partition_table}")


def main():
    partition_table = f"{SOURCE2TABLE[settings.datasource_to_download]}_{settings.ingestion_year}_{settings.ingestion_month:02d}"

    conn_str = (
        f"host={settings.datamart_endpoint} port={settings.datamart_port} dbname={settings.datamart_db} "
        f"user={settings.datamart_user} password={settings.datamart_password}"
    )

    with psycopg.connect(conn_str) as conn:
        if partition_exists(conn, partition_table):
            print(f"ℹ️ Partition already exists: {partition_table}")
        else:
            create_partition(
                conn, 
                settings.datasource_to_download, 
                settings.ingestion_year, 
                settings.ingestion_month
            )


if __name__ == "__main__":
    main()
