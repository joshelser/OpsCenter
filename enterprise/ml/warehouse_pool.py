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


def compute_set_of_warehouses(
    initial_warehouse_size: str, credit_cap: int
) -> pd.DataFrame:
    warehouse_credit_map = get_warehouse_credit_map()
    initial_wh_credits = warehouse_credit_map[initial_warehouse_size]

    if credit_cap < initial_wh_credits:
        raise ValueError("Credit cap is less than the initial warehouse size")

    warehouse_sizes_list = []
    remaining_credits = credit_cap
    while remaining_credits > 0:
        prev_smaller_wh_credits = initial_wh_credits
        prev_larger_wh_credits = initial_wh_credits
        if remaining_credits >= initial_wh_credits:
            warehouse_sizes_list.append(initial_wh_credits)
            remaining_credits -= initial_wh_credits

        while (remaining_credits > 0 and prev_smaller_wh_credits > 1) or (
            remaining_credits > prev_larger_wh_credits and prev_larger_wh_credits < 512
        ):
            if prev_smaller_wh_credits > 1:
                wh_credit = prev_smaller_wh_credits / 2
                if remaining_credits >= wh_credit:
                    warehouse_sizes_list.append(wh_credit)
                    remaining_credits -= wh_credit
                prev_smaller_wh_credits = wh_credit
            if prev_larger_wh_credits < 512:
                wh_credit = prev_larger_wh_credits * 2
                if remaining_credits >= wh_credit:
                    warehouse_sizes_list.append(wh_credit)
                    remaining_credits -= wh_credit
                prev_larger_wh_credits = wh_credit

    warehouse_count_map = dict()
    for credit in warehouse_sizes_list:
        warehouse_count_map[credit] = warehouse_count_map.get(credit, 0) + 1

    warehouse_sizes = []
    num_clusters = []
    for wh_size, wh_credit in warehouse_credit_map.items():
        count = warehouse_count_map.get(wh_credit, 0)
        if count > 0:
            num_clusters.append(count)
            warehouse_sizes.append(wh_size)

    return pd.DataFrame(
        {"warehouse_size": warehouse_sizes, "num_clusters": num_clusters}
    )
