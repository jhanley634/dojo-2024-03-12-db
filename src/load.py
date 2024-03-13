#! /usr/bin/env python
# Copyright 2024 John Hanley. MIT licensed.
"""Loads rows into a database."""
from collections.abc import Iterable
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.orm import Session

from src.et import get_actor_df, get_movie_df


class Loader:
    def __init__(self) -> None:
        db_file = Path("data/imdb.db")
        self.engine = sa.create_engine(f"sqlite:///{db_file}")
        self.movie, self.actor, self.movie_actor = self.create_tables()

    def create_tables(self) -> Iterable[Table]:
        metadata = MetaData()
        movie = Table(
            "movie",
            metadata,
            Column("id", String(9), primary_key=True),
            Column("title", String(200), nullable=False),
            Column("year", Integer(), nullable=False),
            Column("minutes", Integer(), nullable=False),
        )
        actor = Table(
            "actor",
            metadata,
            Column("id", String(9), primary_key=True),
            Column("name", String(200), nullable=False),
            Column("born", Integer(), nullable=False),
            Column("died", Integer()),
        )
        movie_actor = Table(
            "movie_actor",
            metadata,
            Column("movie_id", String(9), primary_key=True),
            Column("actor_id", String(9), primary_key=True),
        )
        metadata.create_all(self.engine, [movie, actor, movie_actor])

        for tbl in metadata.sorted_tables:
            print(tbl)

        return movie, actor, movie_actor

    def load_rows(self):
        get_movie_df().to_sql("movie", self.engine, if_exists="replace")

        with Session(self.engine) as sess:
            sess.execute(self.actor.delete())
            sess.execute(self.movie_actor.delete())
            for _, row in get_actor_df().head().iterrows():
                titles = row.titles
                d = row.to_dict()
                del d["titles"]
                sess.execute(self.actor.insert().values(d))
                for title in titles.split(","):
                    sess.execute(
                        self.movie_actor.insert().values(
                            movie_id=title,
                            actor_id=row.id,
                        )
                    )
                    print(row.id, title)
            sess.commit()


if __name__ == "__main__":
    loader = Loader()
    loader.load_rows()
