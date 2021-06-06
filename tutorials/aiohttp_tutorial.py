# https://www.twilio.com/blog/asynchronous-http-requests-in-python-with-aiohttp
import aiohttp
import asyncio
import time
import requests

def synchronous():

    start_time = time.time()

    for number in range(1, 151):
        url = f'https://pokeapi.co/api/v2/pokemon/{number}'
        resp = requests.get(url)
        pokemon = resp.json()
        print(pokemon['name'], end=',')

    print("\n\n Sync: --- %s seconds ---\n\n" % (time.time() - start_time))


def asynchronous():

    start_time = time.time()

    async def main():

        async with aiohttp.ClientSession() as session:

            for number in range(1, 151):
                pokemon_url = f'https://pokeapi.co/api/v2/pokemon/{number}'
                async with session.get(pokemon_url) as resp:
                    pokemon = await resp.json()
                    print(pokemon['name'], end=", ")

    asyncio.run(main())
    print("\n\nAsync: --- %s seconds ---\n\n" % (time.time() - start_time))



def real_concurrent():

    start_time = time.time()


    async def get_pokemon(session, url):
        async with session.get(url) as resp:
            pokemon = await resp.json()
            return pokemon['name']


    async def main():

        async with aiohttp.ClientSession() as session:

            tasks = []
            for number in range(1, 151):
                url = f'https://pokeapi.co/api/v2/pokemon/{number}'
                tasks.append(asyncio.ensure_future(get_pokemon(session, url)))

            original_pokemon = await asyncio.gather(*tasks)
            for pokemon in original_pokemon:
                print(pokemon, end=", ")

    asyncio.run(main())
    print("\n\nConcurrent: --- %s seconds ---\n\n" % (time.time() - start_time))



synchronous()
asynchronous()
real_concurrent()
