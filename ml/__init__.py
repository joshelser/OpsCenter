import pandas
from typing import Dict, Any, Tuple, Optional, List, Iterable, Hashable, Callable
import numpy as np
from collections import deque
import json


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


def get_hint(df):
    hint_str = df['HINT'].values[0]
    # we assume hint will always be the same across all rows
    try:
        hint = json.loads(hint_str)
    except:
        hint = hint_str

    try:
        if np.isnan(hint):
            hint = {}
    except:
        pass
    if hint is None:
        hint = {}
    if 'epsilon' not in hint:
        hint['epsilon'] = 0.01
    if 'xi' not in hint:
        hint['xi'] = 0.9
    if 'alpha' not in hint:
        hint['alpha'] = 0.9
    if 'gamma' not in hint:
        hint['gamma'] = 0.5

    return hint


def get_return_df(df: pandas.Series, next_wh: str, wsize: str, reward: float, state: int, action: int):
    return {'next_warehouse_size': short_sizes[next_wh] if next_wh is not None else None,
            'warehouse_size': wsize,
            'query_text': df['QUERY_TEXT'],
            'database_name': df['DATABASE_NAME'],
            'schema_name': df['SCHEMA_NAME'],
            'reward': reward,
            'state': state,
            'action': action,
            }


def reward_candidate_a(prev_model_size_idx: int, model_size_idx: int, last_cost: float, cost: float, action: int) -> float:
    r = last_cost - cost  # base reward is difference in last run from this run
    if model_size_idx == prev_model_size_idx:
        # if our model size hasn't changed we give our reward a multiplicative bonus
        # if we increased warehouse size and our model size didn't change this will be a negative reward
        # if we decreased size and model didn't change we will give a positive reward
        # if the warehouse didn't change then the reward should be ~0 and this bonus won't matter
        r *= 2.0
    if prev_model_size_idx > model_size_idx:
        # if we shrunk our model size we give a small bonus for heading in the right direction
        r += 0.1
    return r


class RewardCandidateB:
    def __init__(self):
        self.last_actions = deque(maxlen=4)
        self.last_reward = 0.0

    def __call__(self, prev_model_size_idx: int, model_size_idx: int, last_cost: float, cost: float, action: int) -> float:
        self.last_actions.append(action)
        base_reward = reward_candidate_a(prev_model_size_idx, model_size_idx, last_cost, cost, action)
        total_reward = base_reward
        if self.is_flapping():
            print("flapping")
            d = (self.last_reward + base_reward) * -1.0
            total_reward = base_reward + d
        self.last_reward = base_reward
        return total_reward

    def is_flapping(self) -> bool:
        return len(self.last_actions) == 4 and is_flipping(self.last_actions)

def is_flipping(values):
    # Check for alternating pattern: [0, 1, 0, 1] or [1, 0, 1, 0]
    return all(values[i] != values[i + 1] for i in range(len(values) - 1)) and set(values) == {0, 1}

class QlearningIterate:
    def __init__(self, num_states: int, num_actions: int, max_warehouse_size: Optional[str] = None,
                 reward_func: Callable[[int, int, float, float, int], float] = reward_candidate_a,
                 **hint: Dict[str, Any]):
        self.last_cost: int = 0
        self.last_state: int = 0
        self.last_action: int = 0
        self.max_warehouse_size_idx: int = 9 if max_warehouse_size is None else sizes[max_warehouse_size]
        self.learner = QLearner(num_states=num_states, num_actions=num_actions, **hint)
        self.cost_count_history = np.zeros(num_states)
        self.cost_sum_history = np.zeros(num_states)
        self.reward_func = reward_func

    def next(self, warehouse_size: str, model_size: str, cost: float) -> str:
        self.last_cost = cost
        self.last_state = get_state_from_names(warehouse_size, model_size)
        self.last_action = self.learner.actuate(self.last_state)
        self.cost_sum_history[self.last_state] += cost
        self.cost_count_history[self.last_state] += 1
        return self.next_warehouse_name()

    def historical_cost_per_model_size(self, warehouse_size: str, model_size: str) -> Optional[float]:
        s = get_state_from_names(warehouse_size, model_size)
        if self.cost_count_history[s] == 0:
            return None
        return np.divide(self.cost_sum_history[s], self.cost_count_history[s])

    def historical_cost(self, warehouse_size: str) -> Tuple[Optional[float], Optional[str]]:
        s = [get_state_from_names(warehouse_size, model_size) for model_size in model_sizes]
        if np.sum(self.cost_count_history[s]) == 0:
            return None, None
        c = np.divide(self.cost_sum_history[s], self.cost_count_history[s], out=np.zeros_like(self.cost_sum_history[s]),
                      where=self.cost_count_history[s] != 0)
        idx = np.nanargmax(c)
        return c[idx], model_sizes[idx]

    def iterate(self, warehouse_size: str, model_size: str, cost: float) -> Tuple[str, float]:
        s_prime = get_state_from_names(warehouse_size, model_size)
        s = self.last_state
        a = self.last_action
        r = self.calculate_reward(cost, s_prime)
        self.learner.percept(s, a, s_prime, r)
        self.learner.update_episode()
        return self.next(warehouse_size, model_size, cost), r

    def next_warehouse_name(self) -> str:
        a = self.last_action
        warehouse_size_idx, _ = get_pos_from_state(self.last_state)
        if a == 0:
            # we move up one size (to the max allowed)
            next_wh_idx = min(self.max_warehouse_size_idx, warehouse_size_idx + 1)
        elif a == 1:
            # we move down one size
            next_wh_idx = max(0, warehouse_size_idx - 1)
        else:
            # we stay the same
            next_wh_idx = warehouse_size_idx
        # make sure we stay below the max wh size set by the user
        next_wh = reverse_sizes[next_wh_idx]
        return next_wh

    def calculate_reward(self, cost: float, s_prime: int) -> float:
        _, prev_model_size_idx = get_pos_from_state(self.last_state)
        _, model_size_idx = get_pos_from_state(s_prime)
        r = self.reward_func(prev_model_size_idx, model_size_idx, self.last_cost, cost, self.last_action)
        return r



def end_partition(df: pandas.DataFrame) -> pandas.DataFrame:
    hint = get_hint(df)
    # we aren't using a max warehouse size atm
    max_warehouse_size = None  # df['MAX_WAREHOUSE_SIZE'].values[0]

    model = QlearningIterate(len(sizes) * len(model_sizes), 3, max_warehouse_size, reward_func=RewardCandidateB(), **hint)

    df = iterate_with_restart(model, df.iterrows())
    return df


def iterate(model: QlearningIterate, warehouse_size: str, model_size: str, cost: float, first_run: bool) -> Tuple[
    Optional[str], float]:
    if warehouse_size not in sizes:
        return None, 0.0
    if first_run:
        next_warehouse_size = model.next(warehouse_size, model_size, cost)
        return next_warehouse_size, 0.0
    return model.iterate(warehouse_size, model_size, cost)


def extract_row(row: pandas.Series) -> Tuple[str, str, float]:
    model_size = row['MODEL_RUNTIME_SCORE']
    warehouse_size = row['WAREHOUSE_SIZE']
    cost = float(row['COST'])
    return warehouse_size, model_size, cost


def iterate_with_restart(model: QlearningIterate, df: Iterable[Tuple[Hashable, pandas.Series]]) -> pandas.DataFrame:
    results: List[Dict[str, Any]] = []
    first_warehouse: str = None
    for i, (_, last_row) in enumerate(df):
        try:
            warehouse_size, model_size, cost = extract_row(last_row)
        except:
            results.append(get_return_df(last_row, None, None, 0.0, 0, 0))
            break
        if i == 0:
            first_warehouse = warehouse_size
        next_warehouse_size, reward = iterate(model, warehouse_size, model_size, cost, i == 0)
        results.append(get_return_df(last_row, next_warehouse_size, warehouse_size, reward, model.last_state, model.last_action))
        if reward < -1:
            results.extend(restart(model, first_warehouse, last_row))
            break
    return pandas.DataFrame(results)


def restart(model: QlearningIterate, first_warehouse_size: str, last_row: pandas.Series) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    cost, model_size = model.historical_cost(first_warehouse_size)
    next_warehouse_size = first_warehouse_size
    count = 0
    repeated_no_action = 0
    while cost is not None and count < 1000:
        current_warehouse_size = next_warehouse_size
        next_warehouse_size, reward = model.iterate(next_warehouse_size, model_size, cost)
        cost, model_size = model.historical_cost(next_warehouse_size)
        count += 1
        results.append(get_return_df(last_row, next_warehouse_size, current_warehouse_size, reward, model.last_state, model.last_action))
        if reward < -1:
            cost, model_size = model.historical_cost(first_warehouse_size)
            next_warehouse_size = first_warehouse_size
        if model.last_action == 2:
            repeated_no_action += 1
        else:
            repeated_no_action = 0
        if repeated_no_action > 10:
            break
    return results


class QLearner:
    def __init__(self, num_states, num_actions, alpha=0.9, gamma=0.1, epsilon=0.9, xi=0.99, **kwargs):
        self.num_states = num_states
        self.num_actions = num_actions
        self.alpha = alpha
        self.gamma = gamma
        self.epsilon = epsilon
        self.xi = xi
        self.cur_policy = np.random.randint(num_actions, size=num_states)
        self.q_table = np.copy(qtable)
        # set the policy to be the best action for each state
        for i in range(num_states):
            self.cur_policy[i] = np.argmax(self.q_table[i])

    def percept(self, s: int, a: int, s_prime: int, r: float):
        q_prime = np.max(self.q_table[s_prime])
        old_q_value = self.q_table[s, a]
        learned_value = r + self.gamma * q_prime - old_q_value
        self.q_table[s, a] += self.alpha * learned_value
        self.cur_policy[s] = np.argmax(self.q_table[s])

    def actuate(self, s: int) -> int:
        if np.random.uniform() <= self.epsilon:
            return np.random.randint(self.num_actions)
        else:
            return self.cur_policy[s]

    def update_episode(self):
        self.epsilon *= self.xi


qtable = np.array([[-1., -1., 1.],
                   [0.5, -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [-1., 1., 1.],
                   [0.5, 0.1, 0.5],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [-1., 1., 1.],
                   [0.5, 0.1, 0.5],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [-1., 1., 1.],
                   [0.5, 0.1, 0.5],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [-1., 1., 1.],
                   [0.5, 0.1, 0.5],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [-1., 1., 1.],
                   [0.5, 0.1, 0.5],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [-1., 1., 1.],
                   [0.5, 0.1, 0.5],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [-1., 1., 1.],
                   [0.5, 0.1, 0.5],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [-1., 1., 1.],
                   [0.5, 0.1, 0.5],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [1., -1., 0.01],
                   [-1., 1., 1.],
                   [-1., 0.1, 0.5],
                   [-1., -1., 0.01],
                   [-1., -1., 0.01],
                   [-1., -1., 0.01],
                   [-1., -1., 0.01]])
