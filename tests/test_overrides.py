from data_pipeline_api.overrides import Overrides, Override


def test_find_where_subset():
    override1 = Override(where={"key": "value", "hello": "world"}, use={"override": 1})
    override2 = Override(where={"key": "value"}, use={"override": 2})
    overrides = [override1, override2]
    assert list(Overrides(overrides).find(override1.where)) == overrides


def test_do_not_find_where_superset():
    override1 = Override(where={"key": "value", "hello": "world"}, use={"override": 1})
    override2 = Override(where={"key": "value"}, use={"override": 2})
    overrides = [override1, override2]
    assert list(Overrides(overrides).find(override2.where)) == [override2]


def test_apply_in_order():
    metadata = {"key": "value"}
    Overrides(
        (
            Override(where={"key": "value"}, use={"override": 1}),
            Override(where={"key": "value"}, use={"override": 2}),
        )
    ).apply(metadata)
    assert metadata == {"key": "value", "override": 2}
