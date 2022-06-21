from integration.test_basespark import options, users_schema, arango_datasource_name


def test_read_collection(spark):
    all_opts = {}
    test_options = {
        "table": "users",
        # "protocol": protocol,
        # "contentType": content_type
    }
    for d in [options, test_options]:
        all_opts.update(d)

    df = spark.read\
        .format(arango_datasource_name)\
        .options(**all_opts)\
        .schema(users_schema)\
        .load()

    litalien = df\
        .filter(df.name.first == "Prudence")\
        .filter(df.name.last == "Litalien")

    assert litalien.count() == 1
