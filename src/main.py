import spotify
import asyncio
import sys
import os


# returns an Artist object
async def get_artist(client, name):
	res = await client.search(name, types=['artist'])

	# assume first result is desired result
	try:
		artist = await client.get_artist(str(res['artists'][0]))
	except IndexError as e:
		return False
	return artist


# returns a path of artist IDs from artist1 to artist2
def trace_path(artist1, artist2, parents):
	path = [artist2.name]
	while path[-1] != artist1.name:
		path.append(parents[path[-1]])
	path.reverse()
	return path


def trace_bi_path(artist1, artist2, parents1, parents2, intersection):
	path1 = [intersection.name]
	while path1[-1] != artist1.name:
		path1.append(parents1[path1[-1]])
	path1.reverse()

	path2 = [intersection.name]
	while path2[-1] != artist2.name:
		path2.append(parents2[path2[-1]])
	return path1 + path2[1:]

# find a shortest path through related artists from artist1
# using bidirectional bfs to reduce search space
async def bi_bfs(client, artist1, artist2):
	parent1 = {}
	parent2 = {}
	found = False
	intersect = ""
	queue1 = [artist1]
	queue2 = [artist2]
	queue1_ids = set()
	queue1_ids.add(artist1.id)
	queue2_ids = set()
	queue2_ids.add(artist2.id)
	visited1 = set()
	visited2 = set()
	loop = asyncio.get_event_loop()

	# settings for how often (BFS turns) to display count of artists searched
	status_counter = 0
	status_interval = 4
	while queue1 and queue2 and not found:

		# take turns from each side
		# artist1 side
		current_artist1 = queue1.pop(0)
		queue1_ids.remove(current_artist1.id)
		# if current_artist1.name in parent1:
		# 	print("1: " + parent1[current_artist1.name] + "->" + current_artist1.name)
		# else:
		# 	print("1: " + current_artist1.name)
		if current_artist1.id == artist2.id or current_artist1.id in visited2:
			found = True
			intersect = current_artist1
			break
		if current_artist1.id not in visited1:
			promise = await loop.run_in_executor(None, current_artist1.related_artists)
			related_artists = await promise
			for a in related_artists:
				if not a.name in parent1:
					parent1[a.name] = current_artist1.name
				if not a.id in visited1 and not a.id in queue1_ids:
					queue1.append(a)
					queue1_ids.add(a.id)
			visited1.add(current_artist1.id)

		#artist2 side
		current_artist2 = queue2.pop(0)
		queue2_ids.remove(current_artist2.id)
		# if current_artist2.name in parent2:
		# 	print("2: " + parent2[current_artist2.name] + "->" + current_artist2.name)
		# else:
		# 	print("2: " + current_artist2.name)
		if current_artist2.id == artist1.id or current_artist2.id in visited1:
			found = True
			intersect = current_artist2
			break
		if current_artist2.id not in visited2:
			promise = await loop.run_in_executor(None, current_artist2.related_artists)
			related_artists = await promise
			for a in related_artists:
				if not a.name in parent2:
					parent2[a.name] = current_artist2.name
				if not a.id in visited2 and not a.id in queue2_ids:
					queue2.append(a)
					queue2_ids.add(a.id)
			visited2.add(current_artist2.id)

		if status_counter == 0:
			all_artists = visited1.union(visited2)
			print("Artists searched: {}".format(len(all_artists)))
		status_counter = (status_counter + 1) % status_interval

	if found:
		all_artists = visited1.union(visited2)
		print("Artists searched: {}".format(len(all_artists)))
		return trace_bi_path(artist1, artist2, parent1, parent2, intersect)

	else:
		return False


async def bfs(client, artist1, artist2):
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
	# set client ID and secret in environment variables
	client_ID = ""
	client_secret = ""
	try:
		client_ID = os.environ.get("SIX_DEGREES_CLIENT_ID")
		client_secret = os.environ.get("SIX_DEGREES_CLIENT_SECRET")
	except KeyError as e:
		print("You must set the client ID and secret in SIX_DEGREES_CLIENT_ID and SIX_DEGREES_CLIENT_SECRET (environment variables)")

	# get input and run search
	if client_ID and client_secret:
		client = spotify.Client(client_ID, client_secret)

		artist1_name = ""
		artist2_name = ""

		# allow command line input instead of console
		if len(sys.argv) == 1:
			artist1_name = input("Enter an artist: ")
			artist2_name = input("Enter another artist: ")
		else:
			artist1_name = sys.argv[1]
			artist2_name = sys.argv[2]

		artist1 = await get_artist(client, artist1_name)
		artist2 = await get_artist(client, artist2_name)

		if not artist1:
			print("No artist found named " + artist1_name)
		if not artist2:
			print("No artist found named " + artist2_name)
		if not artist1 or not artist2:
			sys.exit(1)

		print("Calculating...")

		path = await bi_bfs(client, artist1, artist2)
		if path:
			print(path)
		else:
			print("No connection found!")

		await client.close()
	else:
		print("Error with client ID and/or secret")


if __name__ == '__main__':
	loop = asyncio.get_event_loop()
	# loop.set_debug(True)
	loop.run_until_complete(main())
