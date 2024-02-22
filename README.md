# cachedpynetbox

This provides a wrapper around pynetbox that will cache a full copy of the database locally, and monitors the
object-changes endpoint to invalidate objects that have been changed. It provides also a few usefull helperfunctions
and should be mostly a complete dropin replacement for pynetbox.

It can run both in read only mode and 
read write mode. For the read only mode there are three different speed flavours avaible.

## Modes

### Quick mode

In quick mode it will open a file handler to the database and it will keep this file open
until the object gets recreated. This is usefull for short lived scripts.

### Semi Quick mode

This mode it will reopen the database file after the semi quick timer has expired. This will
result in some stale data but the staleness is limited to the amount of the quick timer.

### Non Quick mode

In this mode for each read it will reopen the database this results in the most recent data avaible
but this can result in race conditions when processing the data. For example an interface existed
but does not exist after the second retrieval. This does behave the most similar to the normal netbox
API.

## Usage

```
from cachedpynetbox import pynetbox
nb = pynetbox("URL","token")
```

