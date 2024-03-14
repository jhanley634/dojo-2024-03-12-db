#! /usr/bin/env python
# Copyright 2024 John Hanley. MIT licensed.
# generated by sqlacodegen==3.0.0rc5
"""DB schema"""

from typing import Optional

from sqlalchemy import Index, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Actor(Base):
    __tablename__ = "actor"
    __table_args__ = (Index("actor_born_idx", "born"),)

    id: Mapped[str] = mapped_column(String(10), primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    born: Mapped[int] = mapped_column(Integer)
    died: Mapped[Optional[int]] = mapped_column(Integer)


class Movie(Base):
    __tablename__ = "movie"

    id: Mapped[str] = mapped_column(String(10), primary_key=True)
    title: Mapped[str] = mapped_column(String(200))
    year: Mapped[int] = mapped_column(Integer)
    minutes: Mapped[int] = mapped_column(Integer)


class MovieActor(Base):
    __tablename__ = "movie_actor"
    __table_args__ = (Index("am_idx", "actor_id", "movie_id", unique=True),)

    movie_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    actor_id: Mapped[str] = mapped_column(String(10), primary_key=True)
