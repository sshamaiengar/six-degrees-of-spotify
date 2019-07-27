import spotify
import asyncio
import sys
import os
import redis
import itertools
from typing import NewType, List, Tuple, Dict, Set
from time import time

from custom_types import *
import clients
import cache


def timeit(func):
	async def process(func, *args, **params):
		if asyncio.iscoroutinefunction(func):
			print('this function is a coroutine: {}'.format(func.__name__))
			return await func(*args, **params)
		else:
			print('this is not a coroutine')
			return func(*args, **params)

	async def helper(*args, **params):
		print('{}.time'.format(func.__name__))
		start = time()
		result = await process(func, *args, **params)

		# Test normal function route...
		# result = await process(lambda *a, **p: print(*a, **p), *args, **params)

		print('>>>', (time() - start) * 1000, " ms")
		return result

	return helper


async def get_artist(name: str) -> Artist:
	res = await clients.spotify.search(name, types=['artist'], limit=1)

	# assume first result is desired result
	try:
		artist: Artist = await clients.spotify.get_artist(str(res['artists'][0]))
	except IndexError as e:
		return False
	return artist


async def get_related_artists(artist_id: ArtistID) -> List[ArtistID]:
	if not clients.redis or not cache.get_related_artists(artist_id):
		related = await clients.spotify.http.artist_related_artists(artist_id)
		related_ids: List[ArtistID] = [a['id'] for a in related['artists']]
		cache.store_related_artists(artist_id, related_ids)
		return related_ids
	else:
		return cache.get_related_artists(artist_id)


# for edge case where artist1/2 or is in queue of opposite side when intersection is found
async def trace_path(artist1: Artist, artist2: Artist, parents1: Dict[ArtistID, ArtistID], parents2: Dict[ArtistID, ArtistID]) -> List[ArtistID]:
	# artist1 is the one found from the opposite side
	path = []
	if artist1.id in parents2:
		path = [artist1.id]
		while path[-1] != artist2.id:
			path.append(parents2[path[-1]])
	elif artist2.id in parents1:
		path = [artist2.id]
		while path[-1] != artist1.id:
			path.append(parents1[path[-1]])
		path.reverse()
	return path


async def trace_bi_path(artist1: Artist, artist2: Artist, parents1: Dict[ArtistID, ArtistID], parents2: Dict[ArtistID, ArtistID], intersection) ->  List[ArtistID]:
	path1: List[ArtistID] = [intersection]
	while path1[-1] != artist1.id:
		path1.append(parents1[path1[-1]])
	path1.reverse()

	path2: List[ArtistID] = [intersection]
	while path2[-1] != artist2.id:
		path2.append(parents2[path2[-1]])

	# all names should already be cached at this point
	id_path: List[ArtistID] = path1 + path2[1:]
	return id_path


async def get_name_path(id_path: List[ArtistID]) -> List[str]:
	name_path: List[str] = ["" for i in id_path]
	for i in range(len(id_path)):
		artist = await clients.spotify.get_artist(id_path[i])
		name_path[i] = artist.name
	return name_path


# find a shortest path through related artists from artist1
# using bidirectional bfs to reduce search space
async def bi_bfs(artist1: Artist, artist2: Artist) -> Tuple[List[ArtistID], int]:
	cached_path = cache.get_path(artist1.id, artist2.id)
	if cached_path:
		# if cache.store_longest_path(artist1.id, artist2.id, cached_path):
		# 	print("New longest path")
		if not cache.cached_connection_stats(artist1.id, artist2.id, cached_path):
			print("Error storing cached connection stats")
		return cached_path, 0

	print_progress = False
	parent1: Dict[ArtistID, ArtistID] = {}
	parent2: Dict[ArtistID, ArtistID] = {}
	found = False
	intersect: ArtistID = ""
	queue1: List[ArtistID] = [artist1.id]
	queue2: List[ArtistID] = [artist2.id]
	set1 = set()
	set1.add(artist1.id)
	set2 = set()
	set2.add(artist2.id)
	visited1: Set[ArtistID] = set()
	visited2: Set[ArtistID] = set()
	loop = asyncio.get_event_loop()

	# edge case where artist1/2 or is in queue of opposite side when intersection is found
	# so intersection should be ignored
	one_way_edge_case = False

	# settings for how often (BFS turns) to display count of artists searched
	status_counter = 0
	status_interval = 50
	while queue1 and queue2 and not found:

		# take turns from each side
		current_artist1_id: ArtistID = queue1.pop(0)
		set1.remove(current_artist1_id)
		if current_artist1_id == artist2.id or current_artist1_id in visited2:
			found = True
			intersect = current_artist1_id
			if artist1.id in queue2 or artist2.id in queue1:
				one_way_edge_case = True
			break
		if current_artist1_id not in visited1:
			promise = await loop.run_in_executor(None, lambda: get_related_artists(current_artist1_id))
			related_artists_ids: List[ArtistID] = await promise
			for i in related_artists_ids:
				if i not in parent1:
					parent1[i] = current_artist1_id
				if i not in visited1 and i not in set1:
					queue1.append(i)
					set1.add(i)
			visited1.add(current_artist1_id)

		current_artist2_id: ArtistID = queue2.pop(0)
		set2.remove(current_artist2_id)
		if current_artist2_id == artist1.id or current_artist2_id in visited1:
			found = True
			intersect = current_artist2_id
			if artist1.id in queue2 or artist2.id in queue1:
				one_way_edge_case = True
			break
		if current_artist2_id not in visited2:
			promise = await loop.run_in_executor(None, lambda: get_related_artists(current_artist2_id))
			related_artists_ids: List[ArtistID] = await promise
			for i in related_artists_ids:
				if i not in parent2:
					parent2[i] = current_artist2_id
				if i not in visited2 and i not in set2:
					queue2.append(i)
					set2.add(i)
			visited2.add(current_artist2_id)

		# print progress
		if print_progress:
			if status_counter == 0:
				all_artists = visited1.union(visited2)
				print("Artists searched: {}".format(len(all_artists)-2))
			status_counter = (status_counter + 1) % status_interval

	if found:
		all_artists = visited1.union(visited2)
		# print("Artists searched: {}".format(len(all_artists)-2))
		path: List[ArtistID] = await trace_bi_path(artist1, artist2, parent1, parent2, intersect)
		if one_way_edge_case:
			path2: List[ArtistID] = await trace_path(artist1, artist2, parent1, parent2)
			if len(path2) < len(path):
				path = path2[:]

		# store stats
		# store length, and initialize count associated with this connection
		# update count of artists included in searches
		# if not cache.store_path(artist1.id, artist2.id, path):
		# 	print("Error storing path. May have already been stored")
		if not cache.new_connection_stats(artist1.id, artist2.id, path):
			print("Error updating new connection stats")


		return path, len(all_artists)

	else:
		return [], 0


async def main():
	should_continue = True
	clients.redis = redis.Redis()

	while should_continue:

		# set client ID and secret in environment variables
		client_ID = ""
		client_secret = ""
		try:
			client_ID = os.environ.get("SIX_DEGREES_CLIENT_ID")
			client_secret = os.environ.get("SIX_DEGREES_CLIENT_SECRET")
		except KeyError as e:
			print("You must set the client ID and secret in SIX_DEGREES_CLIENT_ID and SIX_DEGREES_CLIENT_SECRET (environment variables)")
			break

		# get input and run search
		if client_ID and client_secret:
			clients.spotify = spotify.Client(client_ID, client_secret)

			artist1_name = ""
			artist2_name = ""

			# allow command line input instead of console
			if len(sys.argv) == 1:
				artist1_name = input("Enter an artist: ")
				artist2_name = input("Enter another artist: ")

			artist1 = await get_artist(artist1_name)
			artist2 = await get_artist(artist2_name)

			if not artist1:
				print("No artist found named " + artist1_name)
			if not artist2:
				print("No artist found named " + artist2_name)
			if not artist1 or not artist2:
				sys.exit(1)

			print(artist1.id)
			print(artist2.id)

			print("Calculating...")

			id_path, _ = await bi_bfs(artist1, artist2)
			name_path = await get_name_path(id_path)
			if name_path:
				print(" <-> ".join(name_path))
			else:
				print("No connection found!")

			await clients.spotify.close()

			print()
			answer = input("Run again? y/n: ")
			if answer == "y":
				should_continue = True
			else:
				should_continue = False
		else:
			print("Error with client ID and/or secret")
			break


async def run_with_artists(list1, list2):
	clients.redis = redis.Redis()

	# set spotify client ID and secret in environment variables
	client_ID = ""
	client_secret = ""
	try:
		client_ID = os.environ.get("SIX_DEGREES_CLIENT_ID")
		client_secret = os.environ.get("SIX_DEGREES_CLIENT_SECRET")
	except KeyError as e:
		print("You must set the client ID and secret in SIX_DEGREES_CLIENT_ID and SIX_DEGREES_CLIENT_SECRET (environment variables)")

	# get input and run search
	if client_ID and client_secret:
		clients.spotify = spotify.Client(client_ID, client_secret)

		pairs = list(itertools.product(list1, list2))
		# pairs = list(itertools.combinations(list1, 2))
		for p in pairs:
			artist1 = await get_artist(p[0])
			artist2 = await get_artist(p[1])
			id_path, _ = await bi_bfs(artist1, artist2)
			name_path = await get_name_path(id_path)
			if name_path:
				print(artist1.name+"..."+artist2.name+": " + " <-> ".join(name_path))
			else:
				print(artist1.name+"..."+artist2.name+": no connection")

		await clients.spotify.close()


if __name__ == '__main__':
	# 1960s
	# list1 = ['Beatles', 'Rolling Stones', 'Bob Dylan', 'Led Zeppelin', 'Johnny Hallyday', 'Bee Gees', 'Pink Floyd', 'Cher', 'Fleetwood Mac', 'Jackson 5']

	# 1980s
	# list1 = ['Michael Jackson', 'Madonna', 'u2', 'queen', 'ac/dc', 'bruce springsteen', 'bon jovi', 'george michael', 'billy joel', 'Guns n Roses']

	# 1990s
	# list1 = ['celine dion', 'mariah carey', 'whitney houston', 'nirvana', 'michael jackson', 'backstreet boys', 'metallica', 'madonna', 'shania twain', 'guns n roses']

	# 2000s
	# list1 = ['eminem', 'linkin park', 'britney spears', 'coldplay', 'p!nk', 'norah jones', 'nickelback', 'beyonce', 'black eyed peas', 'alicia keys']

	# 2010s
	# list2 = ['adele', 'drake', 'rihanna', 'bruno mars', 'ed sheeran', 'one direction', 'justin bieber', 'taylor swift', 'eminem', 'katy perry']

	loop = asyncio.get_event_loop()
	# loop.set_debug(True)
	loop.run_until_complete(main())
	# loop.run_until_complete(run_with_artists(list1, list2))
