import math
import shutil


def print_divider(message: str = None) -> None:
    size = shutil.get_terminal_size((80, 20))
    if message:
        divider_length = math.floor((size[0] - len(message)) / 2) - 1
        print("=" * divider_length + " " + message + " " + "=" * divider_length)
    else:
        print("=" * size[0])
