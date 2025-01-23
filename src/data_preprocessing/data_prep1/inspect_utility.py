import inspect
import src.data_preprocessing.data_prep2.data_healthcheck as dl
import src.data_preprocessing.data_prep2.main_data_prep2 as du
import pprint

# List all functions in the data_loader module
functions_list = [func for func in inspect.getmembers(dl, inspect.isfunction)]
print("\nAll functions in data_loader module:")
for name, func in functions_list:
    pprint.pprint(name)

# List all functions in the data_utils module
functions_list = [func for func in inspect.getmembers(du, inspect.isfunction)]
print("\nAll functions in data_utils module:")
for name, func in functions_list:
    pprint.pprint(name)
