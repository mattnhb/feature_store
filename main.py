from data_source_parser import DataSourceParser
from data_writer import DataWriter

if __name__ == "__main__":
    df = DataSourceParser().extract()
    df.show(truncate=False)
    df.printSchema()
    # DataWriter().save(df)
