def print_header(label:str) -> None:
    print(f"{label}")
    print("-" * len(label))

def print_seperator(marker:str="-", size:int=5) -> None:
    print(marker * size)