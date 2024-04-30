    return df.withColumn(
    "telefone",
    F.when(
        (F.length(F.col("telefone")) == 10) & (F.substring("telefone", 3, 1).isin("6", "7", "8", "9")),
        F.concat(
            F.substring(F.col("telefone"), 1, 2),
            F.lit("9"),
            F.substring(F.col("telefone"), 3, 8),
        ),
    ).otherwise(F.col("telefone")),
)
