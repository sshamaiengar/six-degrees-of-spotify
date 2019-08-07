from typing import List, Tuple
from custom_types import *
import clients

from redis import RedisError

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


# test Redis connection
def redis_connected():
	try:
		res = clients.redis.ping()
		return True
	except RedisError:
		return False


# cache related artists for a given artist
# if in cache, return value
# else, return False
def get_related_artists(artist_id: ArtistID) -> List[ArtistID]:
	if not redis_connected():
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
	if not redis_connected():
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
	if not redis_connected():
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
	if not redis_connected():
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
	if not redis_connected():
		return []
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
	if not redis_connected():
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


def get_top_artists(max_results: int=5) -> List[ArtistID]:
	if not redis_connected():
		return []
	artist_data = clients.redis.hgetall(ARTIST_SEARCHES_KEY)
	artist_pairs = []
	for k, v in artist_data.items():
		artist_pairs.append([v,k]) # create pairs [value, key] so value can be sorted
	artist_pairs.sort(reverse=True)
	top_artist_ids = [str(p[1],'utf-8') for p in artist_pairs[:max_results]]
	return top_artist_ids


# only do this for unique/new paths
def increase_artist_search_count(artist_id: ArtistID) -> bool:
	if not redis_connected():
		return False
	stat_key = ARTIST_SEARCHES_KEY
	if clients.redis.hincrby(stat_key, artist_id, 1):
		return True
	return False


def get_top_connections(max_results: int=5) -> List[str]:
	if not redis_connected():
		return []
	connection_data = clients.redis.hgetall(CONNECTION_SEARCHES_KEY)
	connection_pairs = []
	for k, v in connection_data.items():
		connection_pairs.append([v, k])  # create pairs [value, key] so value can be sorted
	connection_pairs.sort(reverse=True)
	top_connection_keys = [str(p[1], 'utf-8') for p in connection_pairs[:max_results]]
	return top_connection_keys


def get_nonexistent_connections(max_results: int=5) -> List[str]:
	if not redis_connected():
		return []
	connection_data = clients.redis.hgetall(CONNECTION_LENGTHS_KEY)
	connection_pairs = []
	for k, v in connection_data.items():
		if v == 0:
			connection_pairs.append(k)  # create pairs [value, key] so value can be sorted
	nonexistent_connection_keys = [str(i, 'utf-8') for i in connection_pairs[:max_results]]
	return nonexistent_connection_keys


def get_number_connections_searched() -> int:
	if not redis_connected():
		return -1
	return clients.redis.hlen(CONNECTION_LENGTHS_KEY)


def get_average_degrees_of_separation() -> float:
	if not redis_connected():
		return 0
	connection_lengths = list(map(lambda x: int(x)-1, clients.redis.hvals(CONNECTION_LENGTHS_KEY)))
	if len(connection_lengths) == 0:
		return 0
	return sum(connection_lengths)/len(connection_lengths)


def get_connection_search_count(artist1_id: ArtistID, artist2_id: ArtistID) -> int:
	pass


def increase_connection_search_count(artist1_id: ArtistID, artist2_id: ArtistID) -> bool:
	if not redis_connected():
		return False
	stat_key = CONNECTION_SEARCHES_KEY
	connection_key, _ = get_connection_key(artist1_id, artist2_id)
	if clients.redis.hincrby(stat_key, connection_key, 1):
		return True
	return False


def get_connection_length(artist1_id: ArtistID, artist2_id: ArtistID) -> int:
	pass


def store_connection_length(artist1_id: ArtistID, artist2_id: ArtistID, path: List[ArtistID]) -> bool:
	if not redis_connected():
		return False
	stat_key = CONNECTION_LENGTHS_KEY
	connection_key, _ = get_connection_key(artist1_id, artist2_id)
	if clients.redis.hset(stat_key, connection_key, len(path)):
		return True
	else:
		print("Value already existed in hash")
		return False

# all stats to run when new unique connection is found
def new_connection_stats(artist1_id: ArtistID, artist2_id: ArtistID, path: List[ArtistID]) -> bool:
	if not redis_connected():
		return False
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
	if not redis_connected():
		return False
	good = True
	if not increase_connection_search_count(artist1_id, artist2_id):
		print("Error increasing connection search count")
		good = False
	return good