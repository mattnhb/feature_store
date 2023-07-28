def get_nested_fields(schema, prefix="", delimiter="#"):
    nested_fields = []

    for field in schema.fields:
        full_name = f"{prefix}{field.name}"
        if isinstance(field.dataType, StructType):
            nested_fields.extend(
                get_nested_fields(field.dataType, full_name + delimiter, delimiter)
            )
        else:
            nested_fields.append(full_name)

    return nested_fields
