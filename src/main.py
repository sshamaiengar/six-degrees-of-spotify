import spotify
import asyncio
import sys
import os


# returns an Artist object
async def get_artist(client, name):
	res = await client.search(name, types=['artist'])

	# assume first result is desired result
	artist = await client.get_artist(str(res['artists'][0]))
	return artist


# returns a path of artist IDs from artist1 to artist2
def trace_path(artist1, artist2, parents):
	path = [artist2.name]
	while path[-1] != artist1.name:
		path.append(parents[path[-1]])
	path.reverse()
	return path


# find a shortest path through related artists from artist1
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
		# if current_artist.name in parent:
		# 	print(parent[current_artist.name] + "->" + current_artist.name)
		# else:
		# 	print(current_artist.name)
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

		print("Calculating...")

		path = await bfs(client, artist1, artist2)
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
