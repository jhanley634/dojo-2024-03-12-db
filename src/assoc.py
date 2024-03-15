#! /usr/bin/env python
# Copyright 2024 John Hanley. MIT licensed.
"""Demonstrates FK associations between tables."""
from operator import attrgetter
from pprint import pp

from src.db import get_engine, get_session
from src.model import Actor, Movie, MovieActor


def _get_fred():
    with get_session() as sess:
        # Two ways to win!
        fred = sess.query(Actor).filter_by(id="nm0000001").one()
        assert fred.died == 1987

        fred = sess.query(Actor).where(Actor.id == "nm0000001").one()
        assert fred.died == 1987

        list(fred.movies)  # read them in while we still have an active session

        return fred


def fetch_associated_movies():
    fred = _get_fred()
    for ma in filter(attrgetter("movie"), fred.movies):
        m = ma.movie
        print(ma.movie_id, ma.actor_id, m.year, m.minutes, m.title, "\n")


if __name__ == "__main__":
    fetch_associated_movies()
