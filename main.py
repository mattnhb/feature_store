from data_contract_parser import DataContractParser
from data_writer import DataWriter

if __name__ == "__main__":
    df = DataContractParser().create_snapshots()
    # df.show(truncate=False)
    # df.printSchema()
    # DataWriter().save(df)
