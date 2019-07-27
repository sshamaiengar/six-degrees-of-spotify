from typing import List, Tuple
from custom_types import *
import clients

"""
Cache contents:

<ArtistID> 					-> List of related Artist IDs
<ArtistID>:<ArtistID> 		-> List of Artist IDs in connection
stats:
	longest_path			-> <ArtistID>:<ArtistID> of longest connection
	connection_lengths		-> HASH of <ArtistID>:<ArtistID> -> length
	connection_searches		-> HASH of <ArtistID>:<ArtistID> -> # of searches (unique by origin?)
	artist_searches			-> HASH of <ArtistID> -> # of connections included in
"""

LONGEST_CONNECTION_KEY: str = "stats:longest_path"
CONNECTION_LENGTHS_KEY: str = "stats:connection_lengths"
CONNECTION_SEARCHES_KEY: str = "stats:connection_searches"
ARTIST_SEARCHES_KEY: str = "stats:artist_searches"


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


# get string for connection id/key, as well as boolean of whether order is reversed from input order
def get_connection_key(artistA_id: ArtistID, artistB_id: ArtistID) -> Tuple[str, bool]:
	artist1_id, artist2_id = sorted([artistA_id, artistB_id])
	reverse = False
	if artist2_id == artistA_id:
		reverse = True
	return "{}:{}".format(artist1_id, artist2_id), reverse


# cache paths given two artists (key is "artist1:artist2"
# if in cache, return values
# else, return False
def get_path(artistA_id: ArtistID, artistB_id: ArtistID) -> List[ArtistID]:
	if not clients.redis:
		return []
	# sort to store paths symmetrically (A->B equals B->A)
	path_key, reverse = get_connection_key(artistA_id, artistB_id)
	val: List[bytes] = clients.redis.lrange(path_key, 0, -1)
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
	path_key, reverse = get_connection_key(artistA_id, artistB_id)
	if reverse:
		path = path[::-1]
	if not clients.redis.lrange(path_key, 0, -1):
		for ai in path:
			clients.redis.rpush(path_key, ai)
		return True
	else:
		return False


def get_longest_path() -> List[ArtistID]:
	if not clients.redis:
		return False
	key = LONGEST_CONNECTION_KEY
	longest_path_key = str(clients.redis.get(key), 'utf-8')
	if not longest_path_key:
		return []
	else:
		val = clients.redis.lrange(longest_path_key, 0, -1)
		result: List[ArtistID] = [str(id, 'utf-8') for id in val]
		return result


# stores key of longest path
def store_longest_path(artist1_id: ArtistID, artist2_id: ArtistID, path: List[ArtistID]) -> bool:
	if not clients.redis:
		return False
	key = LONGEST_CONNECTION_KEY
	longest_path = get_longest_path()

	if len(path) >= len(longest_path):
		new_longest_path_key, reverse = get_connection_key(artist1_id, artist2_id)
		if reverse:
			path = path[::-1]
		previous_val = str(clients.redis.get(key), 'utf-8')
		if previous_val != new_longest_path_key:
			clients.redis.set(key, new_longest_path_key)
			return True
	return False


def get_artist_search_count(artist_id: ArtistID) -> int:
	pass


def get_top_artists(max_results: int=5):
	pass

# only do this for unique/new paths
def increase_artist_search_count(artist_id: ArtistID) -> bool:
	stat_key = ARTIST_SEARCHES_KEY
	if clients.redis.hincrby(stat_key, artist_id, 1):
		return True
	return False


def get_top_connections(max_results: int=5) -> List[str]:
	pass


def get_nonexistent_connections(max_results: int=5) -> List[str]:
	pass


def get_average_connection_length() -> float:
	pass


def get_connection_search_count(artist1_id: ArtistID, artist2_id: ArtistID) -> int:
	pass


def increase_connection_search_count(artist1_id: ArtistID, artist2_id: ArtistID) -> bool:
	stat_key = CONNECTION_SEARCHES_KEY
	connection_key, _ = get_connection_key(artist1_id, artist2_id)
	if clients.redis.hincrby(stat_key, connection_key, 1):
		return True
	return False


def get_connection_length(artist1_id: ArtistID, artist2_id: ArtistID) -> int:
	pass


def store_connection_length(artist1_id: ArtistID, artist2_id: ArtistID, path: List[ArtistID]) -> bool:
	stat_key = CONNECTION_LENGTHS_KEY
	connection_key, _ = get_connection_key(artist1_id, artist2_id)
	if clients.redis.hset(stat_key, connection_key, len(path)):
		return True
	else:
		print("Value already existed in hash")
		return False

# all stats to run when new unique connection is found
def new_connection_stats(artist1_id: ArtistID, artist2_id: ArtistID, path: List[ArtistID]) -> bool:
	good = True
	if not store_path(artist1_id, artist2_id, path):
		print("Error storing path. May have already been stored")
		good = False
	if store_longest_path(artist1_id, artist2_id, path):
		print("New longest path")
		good = False
	if not store_connection_length(artist1_id, artist2_id, path):
		print("Error storing connection length")
		good = False
	if not increase_connection_search_count(artist1_id, artist2_id):
		print("Error increasing connection search count")
		good = False
	if not increase_artist_search_count(artist1_id):
		print("Error increasing artist {} search count".format(artist1_id))
		good = False
	if not increase_artist_search_count(artist2_id):
		print("Error increasing artist {} search count".format(artist2_id))
		good = False
	return good


def cached_connection_stats(artist1_id: ArtistID, artist2_id: ArtistID, path: List[ArtistID]) -> bool:
	good = True
	if not increase_connection_search_count(artist1_id, artist2_id):
		print("Error increasing connection search count")
		good = False
	return good