import importlib.util
from pathlib import Path

if __name__ == "__main__":
    # Get the script's directory and construct path to utils module
    script_dir = Path(__file__).parent
    utils_dir = script_dir / "utils"
    data_utils_path = utils_dir / "data_utils.py"
    
    spec = importlib.util.spec_from_file_location("data_utils", data_utils_path)
    data_utils = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(data_utils)

    print(data_utils.generate_fake_data(10))
    print(data_utils.generate_fake_data_with_schema(10))

