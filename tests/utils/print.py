import math
import shutil


def print_divider(message: str = None) -> None:
    size = shutil.get_terminal_size((80, 20))
    if message:
        divider_length = math.floor((size[0] - len(message)) / 2) - 1
        divider_string = (
            "=" * divider_length + " " + message + " " + "=" * divider_length
        )
        if len(divider_string) < size[0]:
            divider_string += "="
        print(divider_string)
    else:
        print("=" * size[0])
