from quart import Quart, Response
import json
from main import *
import clients
from custom_types import *
from quart_cors import cors

app = Quart(__name__)
app = cors(app)


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


async def get_artist_dict(artist_id):
	artist: Artist = await clients.spotify.get_artist(artist_id)
	return generate_artist_dict(artist)

def generate_artist_dict(artist):
	artist_dict: Dict = {}
	artist_dict['name'] = artist.name
	artist_dict['images'] = artist.images
	artist_dict['url'] = 'open.spotify.com/artist/' + artist.id;
	artist_dict['genres'] = artist.genres
	artist_dict['followers'] = artist.followers
	artist_dict['id'] = artist.id
	return artist_dict

if __name__ == '__main__':
	clients.redis = redis.Redis()
	try:
		client_ID = os.environ.get("SIX_DEGREES_CLIENT_ID")
		client_secret = os.environ.get("SIX_DEGREES_CLIENT_SECRET")
	except KeyError as e:
		print(
			"You must set the client ID and secret in SIX_DEGREES_CLIENT_ID and SIX_DEGREES_CLIENT_SECRET (environment variables)")

	if client_ID and client_secret:
		clients.spotify = spotify.Client(client_ID, client_secret)

		app.run()