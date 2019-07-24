from typing import List
from custom_types import *
import clients


# cache artist names
def get_artist_name(artist: Artist) -> str:
	if not clients.redis:
		return ""
	val: bytes = clients.redis.get(artist.id+':name')
	if not val:
		return ""
	else:
		return str(val, 'utf-8')


def store_artist_name(artist: Artist) -> bool:
	if not clients.redis:
		return False
	if not clients.redis.get(artist.id + ':name'):
		clients.redis.set(artist.id+':name', artist.name)
		return True
	else:
		return False


# cache related artists for a given artist
# if in cache, return value
# else, return False
def get_related_artists(artist_id: ArtistID) -> List[ArtistID]:
	if not clients.redis:
		return []

	val: List[bytes] = clients.redis.lrange(artist_id, 0, -1)
	if not val:
		return []
	else:
		res: List[ArtistID] = []
		for id in val:
			str_id: ArtistID = str(id, 'utf-8')
			res.append(str_id)
		return res


# given an artist ID and list of related Artist objects, store in cache
def store_related_artists(artist_id: ArtistID, related_artists_ids: List[ArtistID]) -> bool:
	if not clients.redis:
		return False
	if not clients.redis.lrange(artist_id, 0, -1):
		for i in related_artists_ids:
			clients.redis.rpush(artist_id, i)
		return True
	else:
		return False


# cache paths given two artists (key is "artist1:artist2"
# if in cache, return values
# else, return False
def get_path(artistA_id: ArtistID, artistB_id: ArtistID) -> List[ArtistID]:
	if not clients.redis:
		return []
	# sort to store paths symmetrically (A->B equals B->A)
	artist1_id, artist2_id = sorted([artistA_id, artistB_id])
	reverse = False
	if artist2_id == artistA_id:
		reverse = True
	val: List[bytes] = clients.redis.lrange(artist1_id + ":" + artist2_id, 0, -1)
	if not val:
		return []
	else:
		result: List[ArtistID] = [str(id, 'utf-8') for id in val]
		if reverse:
			return result[::-1]
		else:
			return result


# TODO: what about non-existent paths, how to store
def store_path(artistA_id: ArtistID, artistB_id: ArtistID, path: List[ArtistID]) -> bool:
	if not clients.redis:
		return False
	artist1_id, artist2_id = sorted([artistA_id, artistB_id])
	# reverse path if necessary
	if artist2_id == artistA_id:
		path = path[::-1]
	path_key = artist1_id + ":" + artist2_id
	if not clients.redis.lrange(path_key, 0, -1):
		for ai in path:
			clients.redis.rpush(path_key, ai)
		return True
	else:
		return False
