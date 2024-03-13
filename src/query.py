#! /usr/bin/env python
# Copyright 2024 John Hanley. MIT licensed.
"""Demonstrates JOIN queries."""

import sqlalchemy as sa

from src.db import get_session, get_tables


def example_join() -> None:

    movie, actor, movie_actor = get_tables()

    with get_session() as sess:
        q = (
            sa.select(actor, movie)
            .join(movie_actor, movie_actor.c.actor_id.is_(actor.c.id))
            .join(movie, movie_actor.c.movie_id.is_(movie.c.id))
            .where(actor.c.id < "nm00001")
            .order_by(actor.c.born)
            .limit(90)
        )
        for row in sess.execute(q):
            print(row._asdict())


if __name__ == "__main__":
    example_join()
