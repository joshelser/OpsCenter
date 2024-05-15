from typing import Optional, Dict, Any, Callable, Tuple
import numpy as np
from ml.rewards import reward_candidate_a
from ml.utils import (
    get_state_from_names,
    get_pos_from_state,
    sizes,
    model_run_times,
    reverse_sizes,
)


class QlearningIterate:
    def __init__(
        self,
        num_states: int,
        num_actions: int,
        max_warehouse_size: Optional[str] = None,
        min_warehouse_size: Optional[str] = None,
        reward_func: Callable[
            [int, int, float, float, int], float
        ] = reward_candidate_a,
        **hint: Dict[str, Any]
    ):
        self.last_cost: float = 0
        self.last_state: int = 0
        self.last_action: int = 0
        self.max_warehouse_size_idx: int = (
            9 if max_warehouse_size is None else sizes[max_warehouse_size]
        )
        self.min_warehouse_size_idx: int = (
            0 if min_warehouse_size is None else sizes[min_warehouse_size]
        )
        self.learner = QLearner(num_states=num_states, num_actions=num_actions, **hint)
        self.cost_count_history = np.zeros(num_states)
        self.cost_sum_history = np.zeros(num_states)
        self.reward_func = reward_func

    def next(self, warehouse_size: str, model_run_time: str, cost: float) -> str:
        self.last_cost = cost
        self.last_state = get_state_from_names(warehouse_size, model_run_time)
        self.last_action = self.learner.actuate(self.last_state)
        self.cost_sum_history[self.last_state] += cost
        self.cost_count_history[self.last_state] += 1
        return self.next_warehouse_name()

    def historical_cost_per_model_run_time(
        self, warehouse_size: str, model_run_time: str
    ) -> Optional[float]:
        s = get_state_from_names(warehouse_size, model_run_time)
        if self.cost_count_history[s] == 0:
            return None
        return np.divide(self.cost_sum_history[s], self.cost_count_history[s])

    def historical_cost(
        self, warehouse_size: str
    ) -> Tuple[Optional[float], Optional[str]]:
        s = [
            get_state_from_names(warehouse_size, model_run_time)
            for model_run_time in model_run_times
        ]
        if np.sum(self.cost_count_history[s]) == 0:
            return None, None
        c = np.divide(
            self.cost_sum_history[s],
            self.cost_count_history[s],
            out=np.zeros_like(self.cost_sum_history[s]),
            where=self.cost_count_history[s] != 0,
        )
        idx = np.nanargmax(c)
        return c[idx], model_run_times[idx]

    def iterate(
        self, warehouse_size: str, model_run_time: str, cost: float
    ) -> Tuple[str, float]:
        s_prime = get_state_from_names(warehouse_size, model_run_time)
        s = self.last_state
        a = self.last_action
        r = self.calculate_reward(cost, s_prime)
        self.learner.percept(s, a, s_prime, r)
        self.learner.update_episode()
        return self.next(warehouse_size, model_run_time, cost), r

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
        next_wh_idx = min(next_wh_idx, self.max_warehouse_size_idx)
        # make sure we stay above the min wh size set by the user
        next_wh_idx = max(next_wh_idx, self.min_warehouse_size_idx)

        next_wh = reverse_sizes[next_wh_idx]

        return next_wh

    def calculate_reward(self, cost: float, s_prime: int) -> float:
        _, prev_model_run_time_idx = get_pos_from_state(self.last_state)
        _, model_run_time_idx = get_pos_from_state(s_prime)
        r = self.reward_func(
            prev_model_run_time_idx,
            model_run_time_idx,
            self.last_cost,
            cost,
            self.last_action,
        )
        return r


class QLearner:
    def __init__(
        self,
        num_states,
        num_actions,
        alpha=0.9,
        gamma=0.1,
        epsilon=0.9,
        xi=0.99,
        **kwargs
    ):
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


qtable = np.array(
    [
        [-1.0, -1.0, 1.0],
        [0.5, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [-1.0, 1.0, 1.0],
        [0.5, 0.1, 0.5],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [-1.0, 1.0, 1.0],
        [0.5, 0.1, 0.5],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [-1.0, 1.0, 1.0],
        [0.5, 0.1, 0.5],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [-1.0, 1.0, 1.0],
        [0.5, 0.1, 0.5],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [-1.0, 1.0, 1.0],
        [0.5, 0.1, 0.5],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [-1.0, 1.0, 1.0],
        [0.5, 0.1, 0.5],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [-1.0, 1.0, 1.0],
        [0.5, 0.1, 0.5],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [-1.0, 1.0, 1.0],
        [0.5, 0.1, 0.5],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [1.0, -1.0, 0.01],
        [-1.0, 1.0, 1.0],
        [-1.0, 0.1, 0.5],
        [-1.0, -1.0, 0.01],
        [-1.0, -1.0, 0.01],
        [-1.0, -1.0, 0.01],
        [-1.0, -1.0, 0.01],
    ]
)
