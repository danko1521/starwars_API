import asyncio
import config
from sqlalchemy import Integer, String, Column
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_async_engine(config.db_set_asyncio, echo=False)
Base = declarative_base()


class Characters(Base):
    __tablename__ = 'characters'
    id = Column(Integer, primary_key=True)
    char_id = Column(Integer, index=True)
    name = Column(String, index=True)
    birth_year = Column(String, index=True)
    eye_color = Column(String, index=True)
    films = Column(String, index=True)
    gender = Column(String, index=True)
    hair_color = Column(String, index=True)
    height = Column(String, index=True)
    homeworld = Column(String, index=True)
    mass = Column(String, index=True)
    skin_color = Column(String, index=True)
    species = Column(String, index=True)
    starships = Column(String, index=True)
    vehicles = Column(String, index=True)


async def get_async_session(drop: bool = False, create: bool = False):
    async with engine.begin() as conn:
        if drop:
            await conn.run_sync(Base.metadata.drop_all)
        if create:
            await conn.run_sync(Base.metadata.create_all)
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    return async_session


async def async_session_create():
    await get_async_session(True, True)

    asyncio.run(async_session_create())
