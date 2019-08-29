from quart import Quart, Response, abort
import json
from quart_cors import cors
import urllib.parse as urlparse


from src.main import *
import src.clients as clients
from src.custom_types import *


def init_clients():
	redis_url = None
	try:
		redis_url = os.environ['REDISCLOUD_URL']
	except KeyError as e:
		pass

	if redis_url:
		url = urlparse.urlparse(redis_url)
		clients.redis = redis.Redis(host=url.hostname, port=url.port, password=url.password)
	else:
		clients.redis = redis.Redis()

	client_ID = None
	client_secret = None
	try:
		client_ID = os.environ.get("SIX_DEGREES_CLIENT_ID")
		client_secret = os.environ.get("SIX_DEGREES_CLIENT_SECRET")
	except KeyError as e:
		print("You must set the client ID and secret in SIX_DEGREES_CLIENT_ID and SIX_DEGREES_CLIENT_SECRET (environment variables)")
		return False

	if client_ID and client_secret:
		clients.spotify = spotify.Client(client_ID, client_secret)
	return True

# clients_init = asyncio.run(init_clients())
#
# if not clients_init:
# 	print("Error initiating clients")


def create_app():
	app = Quart(__name__)
	app = cors(app)
	app.spotify = None

	clients_init = init_clients()

	if not clients_init:
		print("Error initiating clients")

	@app.before_request
	def before_request():
		if not app.spotify:
			try:
				client_ID = os.environ.get("SIX_DEGREES_CLIENT_ID")
				client_secret = os.environ.get("SIX_DEGREES_CLIENT_SECRET")
			except KeyError as e:
				print(
					"You must set the client ID and secret in SIX_DEGREES_CLIENT_ID and SIX_DEGREES_CLIENT_SECRET (environment variables)")
				return False
			app.spotify = spotify.Client(client_ID, client_secret)

	# route for getting path given artist IDs
	@app.route('/api/connect/<artist1_id>/<artist2_id>', methods=['GET'])
	async def find_connections(artist1_id, artist2_id):
		artist1: Artist = await app.spotify.get_artist(artist1_id)
		artist2: Artist = await app.spotify.get_artist(artist2_id)
		id_path, artists_searched = await bi_bfs(artist1, artist2)
		artist_dicts = []
		for i in id_path:
			artist_dict = await get_artist_dict(i)
			artist_dicts.append(artist_dict)

		res = artist_dicts

		return Response(json.dumps(res), mimetype='text/json')

	# route for getting search results for web app
	@app.route('/api/search/<artist_name>', methods=['GET'])
	async def search_artists(artist_name):
		results = await app.spotify.search(artist_name, types=['artist'], limit="20")
		artists: List[Artist] = results['artists']
		artist_dicts: List[Dict] = []
		for a in artists:
			artist: Dict = generate_artist_dict(a)
			artist_dicts.append(artist)
		res = artist_dicts
		return Response(json.dumps(res), mimetype='text/json')

	# route for getting one artist (after path found)
	@app.route('/api/artist/<artist_id>', methods=['GET'])
	async def get_artist(artist_id):
		artist: Artist = await app.spotify.get_artist(artist_id)
		artist_dict: Dict = generate_artist_dict(artist)
		return Response(json.dumps(artist_dict), mimetype='text/json')

	@app.route('/api/stats', methods=['GET'])
	async def get_stats():
		if not cache.redis_connected():
			error_message = json.dumps({ "message": "Could not connect to Redis server"})
			abort(500)

		stats = {}
		stats['top_artists'] = []
		stats['top_connections'] = []
		stats['nonexistent_connections'] = []

		top_artist_ids = cache.get_top_artists()
		for i in top_artist_ids:
			res = await get_artist_dict(i)
			stats['top_artists'].append(res)

		top_connection_keys = cache.get_top_connections()
		for i in top_connection_keys:
			connection_dict = dict()
			connection_dict['url'] = i.replace(":", "/")
			connection_dict['artists'] = []
			for j in i.split(":"):
				artist_dict = await get_artist_dict(j)
				connection_dict['artists'].append(artist_dict)
			stats['top_connections'].append(connection_dict)

		stats['mean_degrees'] = cache.get_average_degrees_of_separation()

		stats['connections_searched'] = cache.get_number_connections_searched()

		max_degrees_connection = cache.get_longest_path()
		# just return the artists that identify the connection
		artist_ids = [max_degrees_connection[0], max_degrees_connection[-1]]
		artist_dicts = []
		for i in artist_ids:
			res = await get_artist_dict(i)
			artist_dicts.append(res)
		stats['max_degrees_path'] = {"artists": artist_dicts, "degrees": len(max_degrees_connection)-1, "url": "/".join(artist_ids)}

		nonexistent_connection_keys = cache.get_nonexistent_connections()
		for i in nonexistent_connection_keys:
			connection_dict = dict()
			connection_dict['url'] = i.replace(":", "/")
			connection_dict['artists'] = []
			for j in i.split(":"):
				artist_dict = await get_artist_dict(j)
				connection_dict['artists'].append(artist_dict)
			stats['nonexistent_connections'].append(connection_dict)

		return Response(json.dumps(stats), mimetype='text/json')

	async def get_artist(name: str) -> Artist:
		res = await app.spotify.search(name, types=['artist'], limit=1)

		# assume first result is desired result
		try:
			artist: Artist = await app.spotify.get_artist(str(res['artists'][0]))
		except IndexError as e:
			return False
		return artist

	async def get_related_artists(artist_id: ArtistID) -> List[ArtistID]:
		if not cache.redis_connected() or not cache.get_related_artists(artist_id):
			related = await app.spotify.http.artist_related_artists(artist_id)
			related_ids: List[ArtistID] = [a['id'] for a in related['artists']]
			cache.store_related_artists(artist_id, related_ids)
			return related_ids
		else:
			return cache.get_related_artists(artist_id)

	async def get_artist_dict(artist_id):
		artist: Artist = await app.spotify.get_artist(artist_id)
		return generate_artist_dict(artist)

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
					print("Artists searched: {}".format(len(all_artists) - 2))
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

	return app


def get_image_dicts(images):
	return [{ "url": i.url, "width":i.width, "height": i.height} for i in images]


def generate_artist_dict(artist):
	artist_dict: Dict = {}
	artist_dict['name'] = artist.name
	artist_dict['images'] = get_image_dicts(artist.images)
	artist_dict['url'] = 'open.spotify.com/artist/' + artist.id
	artist_dict['genres'] = artist.genres
	artist_dict['followers'] = artist.followers
	artist_dict['id'] = artist.id
	return artist_dict


if __name__ == '__main__':

	app = create_app()
	app.run()
