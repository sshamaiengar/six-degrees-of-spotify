from quart import Quart, Response, abort
import json
from quart_cors import cors

from src.main import *
import src.clients as clients
from src.custom_types import *


def create_app():
	app = Quart(__name__)
	app = cors(app)

	clients.redis = redis.Redis()

	try:
		client_ID = os.environ.get("SIX_DEGREES_CLIENT_ID")
		client_secret = os.environ.get("SIX_DEGREES_CLIENT_SECRET")
	except KeyError as e:
		print("You must set the client ID and secret in SIX_DEGREES_CLIENT_ID and SIX_DEGREES_CLIENT_SECRET (environment variables)")

	if client_ID and client_secret:
		clients.spotify = spotify.Client(client_ID, client_secret)

	# route for getting path given artist IDs
	@app.route('/api/connect/<artist1_id>/<artist2_id>', methods=['GET'])
	async def find_connections(artist1_id, artist2_id):
		artist1: Artist = await clients.spotify.get_artist(artist1_id)
		artist2: Artist = await clients.spotify.get_artist(artist2_id)
		id_path, artists_searched = await bi_bfs(artist1, artist2)
		artist_dicts = []
		for i in id_path:
			artist_dict = await get_artist_dict(i)
			artist_dicts.append(artist_dict)

		# res = {"artists": name_path, "ids": id_path, "count": artists_searched}
		res = artist_dicts

		return Response(json.dumps(res), mimetype='text/json')

	# route for getting search results for web app
	@app.route('/api/search/<artist_name>', methods=['GET'])
	async def search_artists(artist_name):
		results = await clients.spotify.search(artist_name, types=['artist'], limit=20)
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
		artist: Artist = await clients.spotify.get_artist(artist_id)
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

	return app


async def get_artist_dict(artist_id):
	artist: Artist = await clients.spotify.get_artist(artist_id)
	return generate_artist_dict(artist)


def generate_artist_dict(artist):
	artist_dict: Dict = {}
	artist_dict['name'] = artist.name
	artist_dict['images'] = artist.images
	artist_dict['url'] = 'open.spotify.com/artist/' + artist.id
	artist_dict['genres'] = artist.genres
	artist_dict['followers'] = artist.followers
	artist_dict['id'] = artist.id
	return artist_dict


if __name__ == '__main__':
	# clients.redis = redis.Redis()
	# try:
	# 	client_ID = os.environ.get("SIX_DEGREES_CLIENT_ID")
	# 	client_secret = os.environ.get("SIX_DEGREES_CLIENT_SECRET")
	# except KeyError as e:
	# 	print("You must set the client ID and secret in SIX_DEGREES_CLIENT_ID and SIX_DEGREES_CLIENT_SECRET (environment variables)")
	#
	# if client_ID and client_secret:
	# 	clients.spotify = spotify.Client(client_ID, client_secret)

	app = create_app()
	app.run()
