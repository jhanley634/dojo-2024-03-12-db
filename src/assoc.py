#! /usr/bin/env python
# Copyright 2024 John Hanley. MIT licensed.
"""Demonstrates FK associations between tables."""
from operator import attrgetter

from src.db import get_session
from src.model import Actor, Movie


def _get_fred() -> Actor:
    with get_session() as sess:
        # Two ways to win!
        fred = sess.query(Actor).filter_by(id="nm0000001").one()
        assert fred.died == 1987

        fred = sess.query(Actor).where(Actor.id == "nm0000001").one()
        assert fred.died == 1987

        list(fred.movies)  # read them in while we still have an active session

        return fred


def fetch_associated_movies() -> None:
    for ma in filter(attrgetter("movie"), _get_fred().movies):
        m = ma.movie
        print(ma.movie_id, ma.actor_id, m.year, m.minutes, m.title)


def _get_inferno() -> Movie:
    with get_session() as sess:
        inferno = sess.query(Movie).filter_by(id="tt0072308").one()
        assert inferno.year == 1974
        assert inferno.minutes == 165

        list(inferno.actors)

        return inferno


def fetch_associated_actors() -> None:
    for ma in sorted(
        filter(attrgetter("actor"), _get_inferno().actors),
        key=attrgetter("actor.born"),
    ):
        a = ma.actor
        print(ma.movie_id, ma.actor_id, a.born, a.died, a.name)


if __name__ == "__main__":
    fetch_associated_actors()
    print("\n")
    fetch_associated_movies()
