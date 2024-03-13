#! /usr/bin/env SQLALCHEMY_WARN_20=1 python
# Copyright 2024 John Hanley. MIT licensed.
"""Loads rows into a database."""

from warnings import filterwarnings

from src.db import get_engine, get_session, get_tables
from src.et import get_actor_df, get_movie_df

filterwarnings(
    action="ignore",
    category=UserWarning,
    message="pandas only supports SQLAlchemy connectable",
)


def load_rows() -> None:
    """Populates three DB tables with rows from the parquet files."""

    movie, actor, movie_actor = get_tables()

    with get_session() as sess:
        sess.execute(movie.delete())
    with get_engine().connect() as conn:
        movie_df = get_movie_df().drop(columns=["genres"])
        movie_df.to_sql("movie", conn.connection, if_exists="append", index=False)

    with get_session() as sess:
        sess.execute(actor.delete())
        sess.execute(movie_actor.delete())
        for _, row in get_actor_df().iterrows():
            titles = row.titles
            d = row.to_dict()
            del d["titles"]
            sess.execute(actor.insert().values(d))
            if titles.count(",") >= 4:
                print(row.id, end="\t", flush=True)
                sess.commit()
            for title in titles.split(","):
                sess.execute(
                    movie_actor.insert().values(
                        movie_id=title,
                        actor_id=row.id,
                    )
                )


if __name__ == "__main__":
    load_rows()
