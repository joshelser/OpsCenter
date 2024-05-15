import pandas
from typing import Dict, Any, Tuple, Optional, List, Iterable, Hashable
import numpy as np
import json
from .utils import sizes, model_run_times, size_aliases
from .rewards import RewardCandidateB
from .qlearn import QlearningIterate


def get_hint(df):
    hint_str = df["HINT"].values[0]
    # we assume hint will always be the same across all rows
    try:
        hint = json.loads(hint_str)
    except Exception:
        hint = hint_str

    try:
        if np.isnan(hint):
            hint = {}
    except Exception:
        pass
    if hint is None:
        hint = {}
    if "epsilon" not in hint:
        hint["epsilon"] = 0.01
    if "xi" not in hint:
        hint["xi"] = 0.9
    if "alpha" not in hint:
        hint["alpha"] = 0.9
    if "gamma" not in hint:
        hint["gamma"] = 0.5

    return hint


def get_return_df(
    df: pandas.Series, next_wh: str, wsize: str, reward: float, state: int, action: int
):
    return {
        "next_warehouse_size": size_aliases[next_wh] if next_wh is not None else None,
        "warehouse_size": wsize,
        "query_text": df["QUERY_TEXT"],
        "database_name": df["DATABASE_NAME"],
        "schema_name": df["SCHEMA_NAME"],
        "reward": reward,
        "state": state,
        "action": action,
    }


def end_partition(df: pandas.DataFrame) -> pandas.DataFrame:
    hint = get_hint(df)
    # we aren't using a max warehouse size atm
    max_warehouse_size = df["MAX_WAREHOUSE_SIZE"].values[0]
    min_warehouse_size = df["MIN_WAREHOUSE_SIZE"].values[0]

    model = QlearningIterate(
        len(sizes) * len(model_run_times),
        3,
        max_warehouse_size,
        min_warehouse_size,
        reward_func=RewardCandidateB(),
        **hint
    )

    df = iterate_with_restart(model, df.iterrows())
    return df


def iterate(
    model: QlearningIterate,
    warehouse_size: str,
    model_run_time: str,
    cost: float,
    first_run: bool,
) -> Tuple[Optional[str], float]:
    if warehouse_size not in sizes:
        return None, 0.0
    if first_run:
        next_warehouse_size = model.next(warehouse_size, model_run_time, cost)
        return next_warehouse_size, 0.0
    return model.iterate(warehouse_size, model_run_time, cost)


def extract_row(row: pandas.Series) -> Tuple[str, str, float]:
    model_run_time = row["MODEL_RUNTIME_SCORE"]
    warehouse_size = row["WAREHOUSE_SIZE"]
    cost = float(row["COST"])
    return warehouse_size, model_run_time, cost


def iterate_with_restart(
    model: QlearningIterate, df: Iterable[Tuple[Hashable, pandas.Series]]
) -> pandas.DataFrame:
    results: List[Dict[str, Any]] = []
    first_warehouse: str = None
    for i, (_, last_row) in enumerate(df):
        try:
            warehouse_size, model_run_time, cost = extract_row(last_row)
        except Exception:
            results.append(get_return_df(last_row, None, None, 0.0, 0, 0))
            break
        if i == 0:
            first_warehouse = warehouse_size
        next_warehouse_size, reward = iterate(
            model, warehouse_size, model_run_time, cost, i == 0
        )
        results.append(
            get_return_df(
                last_row,
                next_warehouse_size,
                warehouse_size,
                reward,
                model.last_state,
                model.last_action,
            )
        )
        if reward < -1:
            results.extend(restart(model, first_warehouse, last_row))
            break
    return pandas.DataFrame(results)


def restart(
    model: QlearningIterate, first_warehouse_size: str, last_row: pandas.Series
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    cost, model_run_time = model.historical_cost(first_warehouse_size)
    next_warehouse_size = first_warehouse_size
    count = 0
    repeated_no_action = 0
    while cost is not None and count < 1000:
        current_warehouse_size = next_warehouse_size
        next_warehouse_size, reward = model.iterate(
            next_warehouse_size, model_run_time, cost
        )
        cost, model_run_time = model.historical_cost(next_warehouse_size)
        count += 1
        results.append(
            get_return_df(
                last_row,
                next_warehouse_size,
                current_warehouse_size,
                reward,
                model.last_state,
                model.last_action,
            )
        )
        if reward < -1:
            cost, model_run_time = model.historical_cost(first_warehouse_size)
            next_warehouse_size = first_warehouse_size
        if model.last_action == 2:
            repeated_no_action += 1
        else:
            repeated_no_action = 0
        if repeated_no_action > 10:
            break
    return results
