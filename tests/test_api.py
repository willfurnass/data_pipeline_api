import pathlib
from data_pipeline_api.api import API, DataAccess, ParameterRead
from data_pipeline_api.file_system_data_access import FileSystemDataAccess

DATA_PATH = pathlib.Path(__file__).parent.parent / "example_data"


def test_session_read_estimate():
    api = API(FileSystemDataAccess(DATA_PATH, None))
    assert api.read_estimate("pathogen/SARS-CoV-2/surface-survival") == 2


def test_session_read_distribution():
    api = API(FileSystemDataAccess(DATA_PATH, None))
    distribution = api.read_distribution("human/infection/SARS-CoV-2/latent-period")
    assert distribution.dist.name == "gamma"
    assert distribution.dist._parse_args(*distribution.args, **distribution.kwds) == (
        (3.0,),
        0,
        2.0,
    )


def test_session_read_matrix():
    api = API(FileSystemDataAccess(DATA_PATH, None))
    assert (
        api.read_matrix(
            "human/demographics/scotland", "scotland_2018/grid10k_binned"
        ).sum()
        == 5438100.0
    )


def test_session_read_table():
    api = API(FileSystemDataAccess(DATA_PATH, None))
    table = api.read_table("human/demographics/simple_network_sim")
    assert table["Total"].sum() == 5438100


def test_log_reads():
    api = API(FileSystemDataAccess(DATA_PATH, None))
    api.read_estimate("pathogen/SARS-CoV-2/surface-survival")
    api.read_matrix("human/demographics/scotland", "scotland_2018/grid10k_binned")
    assert api._reads == [
        ParameterRead(
            parameter="pathogen/SARS-CoV-2/surface-survival",
            requested_version=None,
            read_version=1,
            component=None,
            file_hexdigest="3d2e0808478b51fc657cbff2de475ecd4c6118eb401983031da92f29baffdab3",
            verified_hexdigest=None,
        ),
        ParameterRead(
            parameter="human/demographics/scotland",
            requested_version=None,
            read_version=1,
            component="scotland_2018/grid10k_binned",
            file_hexdigest="405dc0eb38a4105318c401d269f24fc82218662b3e0e2aec0dc6f03b194e85bc",
            verified_hexdigest=None,
        ),
    ]
