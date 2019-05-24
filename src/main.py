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


# async def worker(task_queue):
# 	while True:
# 		task = await task_queue.get()
#
# 		task_queue.task_done()

# find a shortest path through related artists from artist1
async def bfs(client, artist1, artist2):
	# track parents to trace final path
	parent = {}
	found = False
	queue = [artist1]
	visited = []
	while queue and not found:
		current_artist = queue.pop(0)
		if current_artist.id == artist2.id:
			found = True
			break
		if current_artist not in visited:
			related_artists = await current_artist.related_artists()
			for a in related_artists:
				if not a.name in parent:
					parent[a.name] = current_artist.name
				queue.append(a)
			visited.append(current_artist.id)

	if found:
		return trace_path(artist1, artist2, parent)


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

		artist1_name = input("Enter an artist: ")
		artist2_name = input("Enter another artist: ")

		artist1 = await get_artist(client, artist1_name)
		artist2 = await get_artist(client, artist2_name)

		print("Calculating...")

		path = await bfs(client, artist1, artist2)
		print(path)

		await client.close()
	else:
		print("Error with client ID and/or secret")


if __name__ == '__main__':
	loop = asyncio.get_event_loop()
	# loop.set_debug(True)
	loop.run_until_complete(main())
