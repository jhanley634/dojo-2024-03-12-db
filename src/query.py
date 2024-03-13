#! /usr/bin/env python
# Copyright 2024 John Hanley. MIT licensed.
"""Demonstrates JOIN queries."""
from typing import Any

import sqlalchemy as sa
from sqlalchemy import Select, TextClause, text

from src.db import get_engine, get_session, get_tables


def example_join() -> None:

    movie, actor, movie_actor = get_tables()

    with get_session() as sess:
        q = (
            sa.select(actor, movie)
            .join(movie_actor, movie_actor.c.actor_id.is_(actor.c.id))
            .join(movie, movie_actor.c.movie_id.is_(movie.c.id))
            .where(movie_actor.c.actor_id < "nm00001")
            .order_by(movie_actor.c.movie_id)
        )
        print("\n".join(map(str, sess.execute(explain(q)))))
        for row in sess.execute(q):
            print(row._asdict())


def explain(q: Select[Any]) -> TextClause:
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
    example_join()
