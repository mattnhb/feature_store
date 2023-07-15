from data_source_parser import DataSourceParser

if __name__ == "__main__":
    df = DataSourceParser().extract()
    df.show(truncate=False)
