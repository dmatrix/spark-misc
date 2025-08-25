import importlib.util
from pathlib import Path

if __name__ == "__main__":
    # Get the script's directory and construct path to utils module
    script_dir = Path(__file__).parent
    utils_dir = script_dir / "utils"
    date_utils_path = utils_dir / "date_utils.py"
    
    spec = importlib.util.spec_from_file_location("date_utils", date_utils_path)
    date_utils = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(date_utils)

    print(date_utils.get_current_date())
    print(date_utils.get_current_time())
    print(date_utils.get_current_datetime())
    print(date_utils.get_current_timestamp())