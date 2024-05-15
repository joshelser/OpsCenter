import pandas
from qlearning import iterate_with_restart, QlearningIterate, RewardCandidateB

sample_data_1 = [
    {
        "WAREHOUSE_SIZE": "X-Small",
        "MODEL_RUNTIME_SCORE": "XL+",
        "COST": 0.2 * 2.36789652,
    },
    {"WAREHOUSE_SIZE": "Small", "MODEL_RUNTIME_SCORE": "XL", "COST": 0.2 * 2.36789652},
    {"WAREHOUSE_SIZE": "Medium", "MODEL_RUNTIME_SCORE": "L", "COST": 0.45 * 2.36789652},
    {"WAREHOUSE_SIZE": "Large", "MODEL_RUNTIME_SCORE": "M", "COST": 2.36789652},
    {"WAREHOUSE_SIZE": "X-Large", "MODEL_RUNTIME_SCORE": "S", "COST": 2.557024272},
    {"WAREHOUSE_SIZE": "2X-Large", "MODEL_RUNTIME_SCORE": "S", "COST": 3.212057928},
    {"WAREHOUSE_SIZE": "3X-Large", "MODEL_RUNTIME_SCORE": "S", "COST": 5.724800448},
    {"WAREHOUSE_SIZE": "4X-Large", "MODEL_RUNTIME_SCORE": "S", "COST": 2 * 5.724800448},
    {
        "WAREHOUSE_SIZE": "5X-Large",
        "MODEL_RUNTIME_SCORE": "S",
        "COST": 4.5 * 5.724800448,
    },
    {"WAREHOUSE_SIZE": "6X-Large", "MODEL_RUNTIME_SCORE": "S", "COST": 9 * 5.724800448},
]

sample_data_2 = [
    {"WAREHOUSE_SIZE": "X-Small", "MODEL_RUNTIME_SCORE": "XS", "COST": 0.000335},
    {"WAREHOUSE_SIZE": "Small", "MODEL_RUNTIME_SCORE": "XS", "COST": 0.000384},
    {"WAREHOUSE_SIZE": "Medium", "MODEL_RUNTIME_SCORE": "XS", "COST": 0.000453},
    {"WAREHOUSE_SIZE": "Large", "MODEL_RUNTIME_SCORE": "XS", "COST": 0.000676},
    {"WAREHOUSE_SIZE": "X-Large", "MODEL_RUNTIME_SCORE": "XS", "COST": 2 * 0.000676},
    {"WAREHOUSE_SIZE": "2X-Large", "MODEL_RUNTIME_SCORE": "XS", "COST": 4 * 0.000676},
    {"WAREHOUSE_SIZE": "3X-Large", "MODEL_RUNTIME_SCORE": "XS", "COST": 8 * 0.000676},
    {"WAREHOUSE_SIZE": "4X-Large", "MODEL_RUNTIME_SCORE": "XS", "COST": 16 * 0.000676},
    {"WAREHOUSE_SIZE": "5X-Large", "MODEL_RUNTIME_SCORE": "XS", "COST": 32 * 0.000676},
    {"WAREHOUSE_SIZE": "6X-Large", "MODEL_RUNTIME_SCORE": "XS", "COST": 64 * 0.000676},
]
sample_data_3 = [
    {"WAREHOUSE_SIZE": "X-Small", "MODEL_RUNTIME_SCORE": "M", "COST": 0.000335},
    {"WAREHOUSE_SIZE": "Small", "MODEL_RUNTIME_SCORE": "S", "COST": 0.000384},
    {"WAREHOUSE_SIZE": "Medium", "MODEL_RUNTIME_SCORE": "XS", "COST": 0.000453},
    {"WAREHOUSE_SIZE": "Large", "MODEL_RUNTIME_SCORE": "XS", "COST": 0.000676},
    {"WAREHOUSE_SIZE": "X-Large", "MODEL_RUNTIME_SCORE": "XS", "COST": 2 * 0.000676},
    {"WAREHOUSE_SIZE": "2X-Large", "MODEL_RUNTIME_SCORE": "XS", "COST": 4 * 0.000676},
    {"WAREHOUSE_SIZE": "3X-Large", "MODEL_RUNTIME_SCORE": "XS", "COST": 8 * 0.000676},
    {"WAREHOUSE_SIZE": "4X-Large", "MODEL_RUNTIME_SCORE": "XS", "COST": 16 * 0.000676},
    {"WAREHOUSE_SIZE": "5X-Large", "MODEL_RUNTIME_SCORE": "XS", "COST": 32 * 0.000676},
    {"WAREHOUSE_SIZE": "6X-Large", "MODEL_RUNTIME_SCORE": "XS", "COST": 64 * 0.000676},
]


def trans(data):
    return {i["WAREHOUSE_SIZE"]: (i["MODEL_RUNTIME_SCORE"], i["COST"]) for i in data}


def get_row(data: dict, wh: str) -> pandas.Series:
    d = {
        "WAREHOUSE_SIZE": wh,
        "MODEL_RUNTIME_SCORE": data[wh][0],
        "COST": data[wh][1],
        "QUERY_TEXT": "",
        "SCHEMA_NAME": "",
        "DATABASE_NAME": "",
    }
    return pandas.Series(d)


def test_ql(items):
    ql = QlearningIterate(
        num_states=60,
        num_actions=3,
        epsilon=0.01,
        alpha=0.9,
        gamma=0.5,
        reward_func=RewardCandidateB(),
    )
    w = "Large"

    def df_iter():
        yield 0, get_row(items, w)
        for _ in range(100):
            next_wh = ql.next_warehouse_name()
            yield 0, get_row(items, next_wh)

    df = iterate_with_restart(ql, df_iter())
    print([ql.historical_cost(i) for i in items.keys()])
    print(df)
    for _, row in df.iterrows():
        d = {
            "next": row["next_warehouse_size"],
            "current": row["warehouse_size"],
            "reward": row["reward"],
            "q": ql.learner.q_table[row["state"]],
            "action": row["action"],
        }
        print(d)
    return df["next_warehouse_size"].values[-1]


if __name__ == "__main__":
    # todo make a better test
    one = test_ql(trans(sample_data_1))
    two = test_ql(trans(sample_data_2))
    three = test_ql(trans(sample_data_3))
    print(one, two, three)
    assert one == "L"
    assert two == "XS"
    assert three == "M"
