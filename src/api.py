from quart import Quart, Response
import json
from main import *
import clients
from custom_types import *

app = Quart(__name__)


# route for getting path given artist IDs
@app.route('/<artist1_id>/<artist2_id>')
async def find_connections(artist1_id, artist2_id):
	artist1: Artist = await clients.spotify.get_artist(artist1_id)
	artist2: Artist = await clients.spotify.get_artist(artist2_id)
	name_path, id_path, artists_searched = await bi_bfs(artist1, artist2)
	res = {"artists": name_path, "ids": id_path, "count": artists_searched}

	return Response(json.dumps(res), mimetype='text/json')

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