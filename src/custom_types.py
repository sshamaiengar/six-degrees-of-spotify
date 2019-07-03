import spotify
from typing import NewType

Artist = NewType('Artist', spotify.Artist)
ArtistID = NewType('ArtistID', str)