from pathlib import Path
import sys
ROOT_PATH = Path(__file__).resolve().parent.parent.parent
SRC_PATH = ROOT_PATH / "src"
sys.path.append(str(ROOT_PATH))
sys.path.append(str(SRC_PATH))


def convert_path_to_linux_format(raw_path: str) -> str:
    """
    Convert a local file path with slashes plus the initial slash
    """

    output =  Path(raw_path).as_posix()
    if not output.startswith("/"):
        output = "/" + output
    return output


if __name__ == "__main__":
    test_path = r"Users\example\documents\file.txt"
    linux_path = convert_path_to_linux_format(test_path)
    print(f"Original path: {test_path}")
    print(f"Linux format path: {linux_path}")