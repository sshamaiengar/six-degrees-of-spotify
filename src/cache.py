import redis
import spotify
import src.clients as clients


# cache artist names
def get_artist_name(artist):
	if not clients.redis:
		return False
	val = clients.redis.get(artist.id+':name')
	if not val:
		return False
	else:
		return str(val, 'utf-8')


def store_artist_name(artist):
	if not clients.redis:
		return False
	if not clients.redis.get(artist.id + ':name'):
		clients.redis.set(artist.id+':name', artist.name)
	else:
		return False


# cache related artists for a given artist
# if in cache, return value
# else, return False
def get_related_artists(artist_id):
	if not clients.redis:
		return False

	val = clients.redis.lrange(artist_id, 0, -1)
	if not val:
		return False
	else:
		return [str(id, 'utf-8') for id in val]


# given an artist ID and list of related Artist objects, store in cache
def store_related_artists(artist_id, related_artists_ids):
	if not clients.redis:
		return False
	if not clients.redis.lrange(artist_id, 0, -1):
		for i in related_artists_ids:
			clients.redis.rpush(artist_id, i)
	else:
		return False


# cache paths given two artists (key is "artist1:artist2"
# if in cache, return values
# else, return False
def get_path(artistA_id, artistB_id):
	if not clients.redis:
		return False
	# sort to store paths symmetrically (A->B equals B->A)
	artist1_id, artist2_id = sorted([artistA_id, artistB_id])
	reverse = False
	if artist2_id == artistA_id:
		reverse = True
	val = clients.redis.lrange(artist1_id + ":" + artist2_id, 0, -1)
	if not val:
		return False
	else:
		result = [str(id, 'utf-8') for id in val]
		if reverse:
			return result[::-1]
		else:
			return result


def store_path(artistA_id, artistB_id, path):
	if not clients.redis:
		return False
	artist1_id, artist2_id = sorted([artistA_id, artistB_id])
	# reverse path if necessary
	if artist2_id == artistA_id:
		path = path[::-1]
	if not clients.redis.lrange(artist1_id + ":" + artist2_id, 0, -1):
		for a in path:
			clients.redis.rpush(artist1_id, a.id)
	else:
		return False
