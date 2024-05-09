
from typing import Tuple

def get_state_from_names(warehouse_size: str, model_size: str) -> int:
    model_size_idx = model_sizes.index(model_size)
    warehouse_size_idx = sizes[warehouse_size]
    return get_state_from_pos((warehouse_size_idx, model_size_idx))


def get_state_from_pos(pos: Tuple[int, int]) -> int:
    return pos[0] * num_cols + pos[1]


def get_pos_from_state(state: int) -> Tuple[int, int]:
    return state // num_cols, state % num_cols


def get_names_from_state(s: int) -> Tuple[str, str]:
    warehouse_size_idx, model_size_idx = get_pos_from_state(s)
    return reverse_sizes[warehouse_size_idx], model_sizes[model_size_idx]


# number of states
# 10 warehouse sizes
sizes = {
    'X-Small': 0,
    'Small': 1,
    'Medium': 2,
    'Large': 3,
    'X-Large': 4,
    '2X-Large': 5,
    '3X-Large': 6,
    '4X-Large': 7,
    '5X-Large': 8,
    '6X-Large': 9,
}
short_sizes = {
    'X-Small': 'XS',
    'Small': 'S',
    'Medium': 'M',
    'Large': 'L',
    'X-Large': 'XL',
    '2X-Large': '2XL',
    '3X-Large': '3XL',
    '4X-Large': '4XL',
    '5X-Large': '5XL',
    '6X-Large': '6XL',
}
reverse_sizes = [k for k, _ in sizes.items()]
# 6 model sizes
model_sizes = ('XS', 'S', 'M', 'L', 'XL', 'XL+')
num_cols = len(model_sizes)

