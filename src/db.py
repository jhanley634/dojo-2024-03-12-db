#! /usr/bin/env python
# Copyright 2024 John Hanley. MIT licensed.
"""Database access utility functions."""
from collections.abc import Iterable
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session

_engine = None


def get_engine() -> Engine:
    global _engine
    db_file = Path("data/imdb.db")
    _engine = _engine or sa.create_engine(f"sqlite:///{db_file}")
    return _engine


def get_session() -> Session:
    return Session(get_engine())


def get_tables() -> Iterable[Table]:
    metadata = MetaData()
    movie = Table(
        "movie",
        metadata,
        Column("id", String(10), primary_key=True),
        Column("title", String(200), nullable=False),
        Column("year", Integer(), nullable=False),
        Column("minutes", Integer(), nullable=False),
    )
    actor = Table(
        "actor",
        metadata,
        Column("id", String(10), primary_key=True),
        Column("name", String(200), nullable=False),
        Column("born", Integer(), nullable=False),
        Column("died", Integer()),
    )
    movie_actor = Table(
        "movie_actor",
        metadata,
        Column("movie_id", String(10), primary_key=True),
        Column("actor_id", String(10), primary_key=True),
    )
    tables = [movie, actor, movie_actor]
    metadata.create_all(get_engine(), tables)
    return tables
