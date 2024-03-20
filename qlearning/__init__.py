import pandas
import numpy as np

# SET statements can theoretically cause cost if they are setting from a select
# CREATE statements can cause cost if we are creating a non SQL proc or func
# Some of the other elements (eg SELECT) may NOT incur cost (eg select 1)
nonzero_cost_statements = {'CALL', 'SELECT', 'COPY', 'CREATE_TABLE_AS_SELECT', 'CREATE', 'EXECUTE_STREAMLIT',
                           'MULTI_STATEMENT', 'MERGE', 'INSERT', 'UPDATE', 'SET', 'UNLOAD', 'DELETE'}
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
reverse_sizes = [k for k, _ in sizes.items()]


def get_state_from_pos(pos):
    return pos[0] * num_cols + pos[1]


def get_pos_from_state(state):
    return state // num_cols, state % num_cols


# number of states
# 10 warehouse sizes
ws_sizes = ('XS', 'S', 'M', 'L', 'XL', '2XL', '3XL', '4XL', '5XL', '6XL')
# 6 model sizes
model_sizes = ('XS', 'S', 'M', 'L', 'XL', 'XL+')
num_states = 10 * 6
num_cols = len(model_sizes)

# num actions: move up a size, move down a size, do nothing
num_actions = 3


def end_partition(df):
    if df.QUERY_TYPE.unique()[0] not in nonzero_cost_statements:
        return pandas.DataFrame({'next_warehouse_size': [None], 'warehouse_size': [None]})

    learner = QLearner(num_states=num_states, num_actions=num_actions, epsilon=0.3, xi=0.90, alpha=0.1, gamma=0.9)
    for i, last_row in df.iterrows():
        mruntime = last_row['MODEL_RUNTIME_SCORE']
        mruntime_idx = model_sizes.index(mruntime)
        wsize = last_row['WAREHOUSE_SIZE']
        if wsize not in sizes:
            return pandas.DataFrame({'next_warehouse_size': [None], 'warehouse_size': [wsize]})
        wsize_idx = sizes[wsize]
        cost = float(last_row['COST'])
        if i == 0:
            s = get_state_from_pos((wsize_idx, mruntime_idx))
            first_cost = cost
            continue
        else:
            # TODO, benefit from staying the same? other benefits
            s_prime = get_state_from_pos((wsize_idx, mruntime_idx))
            a = get_pos_from_state(s_prime)[0] - get_pos_from_state(s)[0]
            a = 0 if a < 0 else 1 if a > 0 else 2
            r = cost - first_cost
            learner.percept(s, a, s_prime, r)
            first_cost = cost
            s = s_prime
            learner.update_episode()
    a = learner.actuate(s)
    if a == 0:
        next_wh = reverse_sizes[min(9, wsize_idx + 1)]
    elif a == 1:
        next_wh = reverse_sizes[max(0, wsize_idx - 1)]
    else:
        next_wh = reverse_sizes[wsize_idx]
    return pandas.DataFrame({'next_warehouse_size': [next_wh], 'warehouse_size': [wsize]})


class QLearner:
    def __init__(self, num_states, num_actions, alpha=0.9, gamma=0.1, epsilon=0.9, xi=0.99):
        self.num_states = num_states
        self.num_actions = num_actions
        self.alpha = alpha
        self.gamma = gamma
        self.epsilon = epsilon
        self.xi = xi
        self.cur_policy = np.random.randint(num_actions, size=num_states)
        self.q_table = np.copy(qtable)

    def percept(self, s, a, s_prime, r):
        q_prime = np.max(self.q_table[s_prime])
        old_q_value = self.q_table[s, a]
        learned_value = r + self.gamma * q_prime - old_q_value
        self.q_table[s, a] += self.alpha * learned_value
        self.cur_policy[s] = np.argmax(self.q_table[s])

    def actuate(self, s):
        if np.random.uniform() <= self.epsilon:
            return np.random.randint(self.num_actions)
        else:
            return self.cur_policy[s]

    def update_episode(self):
        self.epsilon *= self.xi


qtable = np.array([[-1., -1., 1.],
                   [0.5, -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [-1., 1., 1.],
                   [0.5, 0., 0.5],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [-1., 1., 1.],
                   [0.5, 0., 0.5],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [-1., 1., 1.],
                   [0.5, 0., 0.5],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [-1., 1., 1.],
                   [0.5, 0., 0.5],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [-1., 1., 1.],
                   [0.5, 0., 0.5],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [-1., 1., 1.],
                   [0.5, 0., 0.5],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [-1., 1., 1.],
                   [0.5, 0., 0.5],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [-1., 1., 1.],
                   [0.5, 0., 0.5],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [1., -1., 0.],
                   [-1., 1., 1.],
                   [-1., 0., 0.5],
                   [-1., -1., 0.],
                   [-1., -1., 0.],
                   [-1., -1., 0.],
                   [-1., -1., 0.]])
