
from base import Base

class Label(Base):
    def __init__(self, name: str, condition: str, group_name: str=None, group_rank: int=None, dynamic: bool=False):
        super().__init__('internal', 'labels')
        self.name = name
        self.condition = condition
        self.group_name = group_name
        self.group_rank = group_rank
        self.dynamic = dynamic

    def create(self):
        if self.dynamic:
            if self.group_name is None:
                raise ValueError('Group name is required for dynamic grouped labels.')
            elif self.name:
                raise ValueError('Name must not be set for dynamic grouped labels.')
            elif self.group_rank:
                raise ValueError('Group rank must not be set for dynamic grouped labels.')
        else:
            if not self.name:
                raise ValueError('Label name is required.')
            elif (self.group_name and not self.group_rank) or (not self.group_name and self.group_rank):
                raise ValueError('Grouped labels require both a group name and group rank.')

        self.validate()

    def validate(self) -> str:
        if self.group_name:
            self.validate_group()
        else:
            self.validate_name()
        self.validate_condition()

    def validate_name(self):
        """
        -- check if the ungrouped label's name conflict with another ungrouped label, or a group with same name.
        """
        pass

    def validate_group(self):
        """
            -- check if the grouped label's name conflict with :
            --  1) another label in the same group,
            --  2) or an ungrouped label's name.
            --  3) another dynamic group name
        Validate the group name is not a column in enriched_query_history
        """
        pass

    def validate_condition(self):
        """"
        Validate the condition string is a valid WHERE clause
        """
        pass
