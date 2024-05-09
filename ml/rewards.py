
from collections import deque

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
