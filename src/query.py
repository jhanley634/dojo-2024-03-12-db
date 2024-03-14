#! /usr/bin/env python
# Copyright 2024 John Hanley. MIT licensed.
"""Demonstrates JOIN queries."""
from typing import Any

import sqlalchemy as sa
from sqlalchemy import Row, Select, TextClause, text
from sqlalchemy.orm import Session

from src.db import get_engine, get_session, get_tables


def example_join_query() -> None:

    movie, actor, movie_actor = get_tables()

    with get_session() as sess:
        q = (
            sa.select(actor, movie)
            .join(movie_actor, movie_actor.c.actor_id.is_(actor.c.id))
            .join(movie, movie_actor.c.movie_id.is_(movie.c.id))
            .where(movie_actor.c.actor_id.is_("nm0000001"))
            .order_by(movie_actor.c.movie_id)
        )
        print(QueryPlan(q, sess), "\n\n")
        for row in sess.execute(q):
            print(row._asdict())


class QueryPlan:
    def __init__(self, q: Select[Any], sess: Session) -> None:
        self.compiled_query = sess.execute(_get_explanation(q))

    def __str__(self) -> str:
        def fmt_row(row: Row[Any]) -> str:
            sqlite_plan_fields = ["id", "parent", "notused", "detail"]
            d = row._asdict()
            assert sqlite_plan_fields == list(d.keys()), d
            return f"{d['id']:3d} {d['parent']:2d}:   {d['detail']}"

        return "\n".join(map(fmt_row, self.compiled_query))


def _get_explanation(q: Select[Any]) -> TextClause:
    """Obtains a query plan."""
    compiled = str(
        q.compile(
            get_engine(),
            dialect=get_engine().dialect,
            compile_kwargs={"literal_binds": True},
        )
    )
    return text(f"EXPLAIN QUERY PLAN {compiled}")


if __name__ == "__main__":
    example_join_query()
