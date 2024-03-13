#! /usr/bin/env python
# Copyright 2024 John Hanley. MIT licensed.
"""Loads rows into a database."""
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy import Column, Integer, MetaData, String, Table, text
from sqlalchemy.orm import Session

from src.et import get_actor_df, get_movie_df


class Loader:
    def __init__(self) -> None:
        db_file = Path("data/imdb.db")
        self.engine = sa.create_engine(f"sqlite:///{db_file}")
        self.movie, self.actor, self.movie_actor = self.create_tables()

    def create_tables(self) -> None:
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
        movie_df = get_movie_df()
        # movie_df.to_sql("movie", self.engine, if_exists="replace")

        actor_df = get_actor_df()
        actor = self.actor

        with Session(self.engine) as sess:
            sess.execute(actor.delete())
            for _, row in actor_df.head().iterrows():
                d = row.to_dict()
                del d["died"]
                del d["titles"]
                print(d, "\t", row.born, "\t", d["name"])
                sess.execute(actor.insert().values(d))
                # sess.execute(actor.insert().values(id=d["id"], name=d["name"]))
            sess.commit()


if __name__ == "__main__":
    loader = Loader()
    loader.load_rows()
