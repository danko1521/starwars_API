import aiohttp
import asyncio
import requests
import time
import sys
from more_itertools import chunked
from typing import Iterable
from sqlalchemy import insert
from aiopg.sa import create_engine
import config
from db import Characters, get_async_session

response = requests.get('https://swapi.dev/api/people/')
quantity = (response.json()['count'])
print(f'Всего персонажей - {quantity}, Выполняется загрузка...')


async def get_chars(session: aiohttp.client.ClientSession, range_char_id: Iterable[int]):
    chars_list = []
    for char_id in range_char_id:
        res = await get_char(session, char_id)
        chars_list.append(res)
    for char_id_chunk in chunked(range_char_id, 10):
        get_char_tasks = [asyncio.create_task(get_char(session, char_id)) for char_id in char_id_chunk]
        await asyncio.gather(*get_char_tasks)
    return chars_list


async def get_char(session: aiohttp.client.ClientSession, char_id: int) -> dict:
    async with session.get(f'https://swapi.dev/api/people/{char_id}', ssl=False) as response:
        response_json = await response.json()
        try:
            response_json['id'] = char_id
            response_json.pop('created')
            response_json.pop('edited')
            response_json.pop('url')
        except KeyError:
            response_json['id'] = char_id
        return response_json


async def add_char(char):
    async with create_engine(config.db_set) as engine:
        async with engine.acquire() as conn:
            try:
                req = insert(Characters).values(char_id=char['id'], name=char['name'],
                                                height=char['height'], mass=char['mass'],
                                                hair_color=char['hair_color'], skin_color=char['skin_color'],
                                                eye_color=char['eye_color'], birth_year=char['birth_year'],
                                                gender=char['gender'], homeworld=char['homeworld'],
                                                films=char['films'], species=char['species'],
                                                vehicles=char['vehicles'], starships=char['starships'])
                print(char['id'], ":", char['name'], char['birth_year'])
                await conn.execute(req)
            except KeyError:
                req = insert(Characters).values()
                await conn.execute(req)
            except ValueError:
                req = insert(Characters).values()
                await conn.execute(req)


async def add_chars():
    async with aiohttp.client.ClientSession() as session:
        chars_list = await get_chars(session, range(1, quantity + 1))
        for char in chars_list:
            await add_char(char)


async def main():
    async with aiohttp.client.ClientSession() as session:
        await get_chars(session, range(1, quantity + 1))
        await get_async_session(True, True)
        await add_chars()
        time.sleep(0.1)


if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

if __name__ == '__main__':
    start = time.time()
    asyncio.run(main())
    print('_' * 50)
    print(f'{quantity} персонажа добавлены в базу данных за {time.time() - start} секунд')
