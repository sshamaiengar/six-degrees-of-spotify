import spotify
import asyncio
import sys
import os
import src.cache as cache
import redis
import src.clients as clients
from functools import partial
import itertools


# returns an Artist object
async def get_artist(name):
	res = await clients.spotify.search(name, types=['artist'])

	# assume first result is desired result
	try:
		artist = await clients.spotify.get_artist(str(res['artists'][0]))
	except IndexError as e:
		return False
	return artist


async def get_related_artists(artist_id):
	if not clients.redis or not cache.get_related_artists(artist_id):
		related = await clients.spotify.http.artist_related_artists(artist_id)
		related_ids = [a['id'] for a in related['artists']]
		cache.store_related_artists(artist_id, related_ids)
		return related_ids
	else:
		return cache.get_related_artists(artist_id)


# returns a path of artist IDs from artist1 to artist2
def trace_path(artist1, artist2, parents):
	path = [artist2.name]
	while path[-1] != artist1.name:
		path.append(parents[path[-1]])
	path.reverse()
	return path


async def trace_bi_path(artist1, artist2, parents1, parents2, intersection):
	path1 = [intersection]
	while path1[-1] != artist1.id:
		path1.append(parents1[path1[-1]])
	path1.reverse()

	path2 = [intersection]
	while path2[-1] != artist2.id:
		path2.append(parents2[path2[-1]])

	# all names should already be cached at this point
	path = path1 + path2[1:]
	for i in range(len(path)):
		artist = await clients.spotify.get_artist(path[i])
		path[i] = artist.name
	return path


# find a shortest path through related artists from artist1
# using bidirectional bfs to reduce search space
async def bi_bfs(artist1, artist2):
	print_progress= False
	parent1 = {}
	parent2 = {}
	found = False
	intersect = ""
	queue1 = [artist1.id]
	queue2 = [artist2.id]
	set1 = set()
	set1.add(artist1.id)
	set2 = set()
	set2.add(artist2.id)
	visited1 = set()
	visited2 = set()
	loop = asyncio.get_event_loop()

	# settings for how often (BFS turns) to display count of artists searched
	status_counter = 0
	status_interval = 50
	while queue1 and queue2 and not found:

		# take turns from each side
		current_artist1_id = queue1.pop(0)
		set1.remove(current_artist1_id)
		if current_artist1_id == artist2.id or current_artist1_id in visited2:
			found = True
			intersect = current_artist1_id
			break
		if current_artist1_id not in visited1:
			promise = await loop.run_in_executor(None, lambda: get_related_artists(current_artist1_id))
			related_artists_ids = await promise
			for i in related_artists_ids:
				if i not in parent1:
					parent1[i] = current_artist1_id
				if i not in visited1 and i not in set1:
					queue1.append(i)
					set1.add(i)
			visited1.add(current_artist1_id)

		current_artist2_id = queue2.pop(0)
		set2.remove(current_artist2_id)
		if current_artist2_id == artist1.id or current_artist2_id in visited1:
			found = True
			intersect = current_artist2_id
			break
		if current_artist2_id not in visited2:
			promise = await loop.run_in_executor(None, lambda: get_related_artists(current_artist2_id))
			related_artists_ids = await promise
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
		print("Artists searched: {}".format(len(all_artists)-2))
		return await trace_bi_path(artist1, artist2, parent1, parent2, intersect)

	else:
		return False


async def bfs(artist1, artist2):
	# track parents to trace final path
	parent = {}
	found = False
	queue = [artist1]
	queue_ids = set()
	queue_ids.add(artist1.id)
	visited = set()
	while queue and not found:
		current_artist = queue.pop(0)
		queue_ids.remove(current_artist.id)
		if current_artist.name in parent:
			print(parent[current_artist.name] + "->" + current_artist.name)
		else:
			print(current_artist.name)
		if current_artist.id == artist2.id:
			found = True
			break
		if current_artist.id not in visited:
			# run parallel requests for all related artists at same level (have same parent)
			loop = asyncio.get_event_loop()
			promise = await loop.run_in_executor(None, current_artist.related_artists)
			related_artists = await promise
			for a in related_artists:
				if not a.name in parent:
					parent[a.name] = current_artist.name
				if not a.id in visited and not a.id in queue_ids:
					queue.append(a)
					queue_ids.add(a.id)
			visited.add(current_artist.id)

	if found:
		return trace_path(artist1, artist2, parent)

	else:
		return False


async def main():
	should_continue = True
	global clients
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

			print("Calculating...")

			path = await bi_bfs(artist1, artist2)
			if path:
				print(" <-> ".join(path))
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
	global clients
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
			path = await bi_bfs(artist1, artist2)
			if path:
				print(artist1.name+"..."+artist2.name+": " + " <-> ".join(path))
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
