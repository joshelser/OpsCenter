import pytest
from datetime import datetime

import snowflake.snowpark.exceptions

from .labels import Label
from .session import session_ctx


class Session:
    def __init__(self):
        self._sql = []

    def sql(self, sql):
        self._sql.append(sql)
        return self

    def collect(self):
        # GROSS. Tricks the tests into passing the check that a label name doesn't conflict with a QUERY_HISTORY column.
        # but only trying to match the name check and not the condition check.
        if self.sql and self._sql[-1].endswith('from reporting.enriched_query_history where false') and self._sql[-1].startswith('select "'):
            raise snowflake.snowpark.exceptions.SnowparkSQLException('invalid identifier to make tests pass')
        return self


@pytest.fixture(autouse=True)
def session():
    session = Session()
    token = session_ctx.set(session)
    yield session
    session_ctx.reset(token)


def _get_label(name="label1", group_name=None, group_rank=None, condition="user_name = 'josh@sundeck.io'", dynamic=False) -> dict:
    d = dict(
        name=name,
        condition=condition,
        label_modified_at=datetime.now(),
        label_created_at=datetime.now(),
        enabled=True,
        is_dynamic=dynamic,
    )
    if dynamic:
        del d['name']
        d['group_name'] = name
    elif group_name and group_rank:
        d['group_name'] = group_name
        d['group_rank'] = group_rank

    return d


def test_label(session):
    l = _get_label()
    _ = Label.parse_obj(l)

    assert len(session._sql) == 2, f"Expected 2 sql statements"
    assert session._sql[0].lower() == _expected_condition_check_query(l.get('condition')), \
        "Unexpected label condition query"
    assert session._sql[1].lower() == _expected_name_check_query(l.get('name')), "Unexpected label name query"


def test_none_label(session):
    l = _get_label(name=None)
    with pytest.raises(ValueError):
        _ = Label.parse_obj(l)

    assert len(session._sql) == 2, "Expected no sql statements for a None name"
    assert session._sql[0].lower() == _expected_condition_check_query(l.get('condition')), \
        "Unexpected label condition query"
    assert session._sql[1].lower() == _expected_name_check_query(l.get('name')), "Unexpected label name query"


def test_empty_label(session):
    l = _get_label(name="")
    with pytest.raises(ValueError):
        _ = Label.parse_obj(l)

    assert len(session._sql) == 2, "Expected no sql statements for a None name"
    assert session._sql[0].lower() == _expected_condition_check_query(l.get('condition')), \
        "Unexpected label condition query"
    # An empty name is overriden to be the default value None.
    assert session._sql[1].lower() == _expected_name_check_query(''), "Unexpected label name query"


def test_missing_condition(session):
    l = _get_label()
    l['condition'] = ''
    with pytest.raises(ValueError):
        _ = Label.parse_obj(l)

    del l['condition']
    with pytest.raises(ValueError):
        _ = Label.parse_obj(l)


def test_missing_created_at(session):
    l = _get_label(name="")
    del l['label_created_at']
    with pytest.raises(ValueError):
        _ = Label.parse_obj(l)


@pytest.mark.parametrize('column', [('label_created_at'), ('label_modified_at')])
def test_fail_when_times_are_not_times(column):
    l = _get_label()
    l[column] = 'not a time'
    with pytest.raises(ValueError):
        _ = Label.parse_obj(l)

    # some other kind of junk
    l[column] = (1234, 5678)
    with pytest.raises(ValueError):
        _ = Label.parse_obj(l)


def test_missing_modified_at(session):
    l = _get_label(name="")
    del l['label_modified_at']
    with pytest.raises(ValueError):
        _ = Label.parse_obj(l)


def test_create_table(session):
    Label.create_table(session)
    assert len(session._sql) == 2, "Expected 2 sql statement, got {}".format(
        len(session._sql)
    )
    assert session._sql[0].lower() == " ".join(
        """create table if not exists internal.labels
        (name string null, group_name string null, group_rank number null,
        label_created_at timestamp, condition string, enabled boolean, label_modified_at timestamp,
        is_dynamic boolean)""".split()
    ), "Expected create table statement, got {}".format(session._sql[0])
    assert (
        session._sql[1].lower()
        == "create or replace view catalog.labels as select * from internal.labels"
    ), "Expected create view statement, got {}".format(session._sql[1])


def _expected_condition_check_query(condition: str) -> str:
    return f"select case when {condition} then 1 else 0 end from reporting.enriched_query_history where false".lower()


def _expected_name_check_query(name: str) -> str:
    return f'select "{name}" from reporting.enriched_query_history where false'.lower()