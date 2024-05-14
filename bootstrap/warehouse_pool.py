import pandas as pd


def get_warehouse_credit_map():
    return {
        "X-Small": 1,
        "Small": 2,
        "Medium": 4,
        "Large": 8,
        "X-Large": 16,
        "2X-Large": 32,
        "3X-Large": 64,
        "4X-Large": 128,
        "5X-Large": 256,
        "6X-Large": 512,
    }


def get_warehouse_size_strings():
    return ["X-Small", "Small", "Medium", "Large", "X-Large",
            "2X-Large", "3X-Large", "4X-Large", "5X-Large", "6X-Large"]


def compute_set_of_warehouses(initial_warehouse_size: str, credit_cap: int) -> pd.DataFrame:
    warehouse_credit_map = get_warehouse_credit_map()
    initial_wh_credits = warehouse_credit_map.get(initial_warehouse_size, "Invalid Warehouse Size")
    if initial_wh_credits == "Invalid Warehouse Size":
        raise ValueError("Invalid Warehouse Size")

    if credit_cap < initial_wh_credits:
        raise ValueError("Credit cap is less than the initial warehouse size")

    warehouse_sizes_str = ""
    remaining_credits = credit_cap
    while remaining_credits > 0:
        prev_smaller_wh_credits = initial_wh_credits
        prev_larger_wh_credits = initial_wh_credits
        if remaining_credits >= initial_wh_credits:
            warehouse_sizes_str += str(initial_wh_credits) + ", "
            remaining_credits -= initial_wh_credits

        while (remaining_credits > 0 and prev_smaller_wh_credits > 1) or (
                remaining_credits > prev_larger_wh_credits and prev_larger_wh_credits < 512):
            if prev_smaller_wh_credits > 1:
                wh_size = int(prev_smaller_wh_credits / 2)
                if remaining_credits >= wh_size:
                    warehouse_sizes_str += str(wh_size) + ", "
                    remaining_credits -= wh_size
                prev_smaller_wh_credits = wh_size
            if prev_larger_wh_credits < 512:
                wh_size = int(prev_larger_wh_credits * 2)
                if remaining_credits >= wh_size:
                    warehouse_sizes_str += str(wh_size) + ", "
                    remaining_credits -= wh_size
                prev_larger_wh_credits = wh_size

    warehouse_count_map = dict()
    for credit_str in warehouse_sizes_str.rstrip(', ').split(","):
        credit_str = credit_str.rstrip(' ')
        if credit_str == "":
            continue
        credit = int(credit_str)
        warehouse_count_map[credit] = warehouse_count_map.get(credit, 0) + 1

    warehouse_sizes = []
    num_clusters = []
    warehouse_size_strings = get_warehouse_size_strings()
    for i, wh_credits in enumerate([1, 2, 4, 8, 16, 32, 64, 128, 256, 512]):
        count = warehouse_count_map.get(wh_credits, 0)
        if count > 0:
            num_clusters.append(count)
            warehouse_sizes.append(warehouse_size_strings[i])

    return pd.DataFrame({'warehouse_size': warehouse_sizes, 'num_clusters': num_clusters})
