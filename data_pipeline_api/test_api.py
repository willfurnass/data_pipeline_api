import pytest
from api import Session, DataLayer, ParameterRead


@pytest.mark.xfail(reason="Hashes are not yet correct")
def test_session_get_hexdigest():
    data_layer = DataLayer("repos/temporary_data")
    session = Session(data_layer)
    metadata = data_layer.get_parameter_metadata("pathogen/SARS-CoV-2/surface-survival")
    assert session.get_hexdigest(metadata) == "www"


def test_session_is_data_verified():
    data_layer = DataLayer("repos/temporary_data")
    session = Session(data_layer)
    metadata = data_layer.get_parameter_metadata("pathogen/SARS-CoV-2/surface-survival")
    assert not session.is_data_verified(metadata)


def test_session_read_param():
    session = Session(DataLayer("repos/temporary_data"))
    assert session.read_param("pathogen/SARS-CoV-2/surface-survival") == 2


def test_session_read_matrix():
    session = Session(DataLayer("repos/temporary_data"))
    assert (
        session.read_matrix(
            "human/demographics/scotland", "scotland_2018/grid10k_binned"
        ).sum()
        == 5438100.0
    )


def test_log_reads():
    session = Session(DataLayer("repos/temporary_data"))
    session.read_param("pathogen/SARS-CoV-2/surface-survival")
    session.read_matrix("human/demographics/scotland", "scotland_2018/grid10k_binned")
    assert session.log_reads() == [
        ParameterRead(
            parameter="pathogen/SARS-CoV-2/surface-survival",
            component=None,
            version=None,
            verified=False,
        ),
        ParameterRead(
            parameter="human/demographics/scotland",
            component="scotland_2018/grid10k_binned",
            version=None,
            verified=False,
        ),
    ]


def test_data_layer_get_parameter_metadata():
    data_layer = DataLayer("repos/temporary_data")
    metadata = data_layer.get_parameter_metadata("pathogen/SARS-CoV-2/surface-survival")
    assert metadata.data_type.value == "parameter"
    assert metadata.location == "pathogen/SARS-CoV-2/surface-survival/data/1.toml"
    assert metadata.hexdigest == "www"


def test_data_layer_get_data_file():
    data_layer = DataLayer("repos/temporary_data")
    metadata = data_layer.get_parameter_metadata("pathogen/SARS-CoV-2/surface-survival")
    with data_layer.get_data_file(metadata) as data_file:
        assert data_file.read() == b"[point-estimate]\nvalue = 2.0\n"
