import pathlib
from scipy import stats
from data_pipeline_api.api import API, DataAccess, ParameterRead
from data_pipeline_api.file_system_data_access import FileSystemDataAccess

data_path = "../simple_network_sim/data_pipeline_inputs"
api = API(FileSystemDataAccess(data_path, "metadata.toml"))
population = api.read_table("human/population")

print(f"Population is {population['Total'].sum()}")

api.write_table(population, "output/table", version=1)
api.close()
