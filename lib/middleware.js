
var dandy = require('dandy/errors');
var _ = require('underscore');
var buffers = require('buffers');

// Whitelist of headers to cache - should it be a blacklist instead?
exports.headersToCache = [
	"content-type",
	"etag",
	"cache-control"
];

// Blacklist of mime types not to gzip
exports.gzipBlacklist = [];

var reContentType = /^(.*?);\s+charset=(.*?)$/i;

// *************************************************************************************************

exports.middleware = function(cache) {
	return function(req, res, next) {
		try {
			var cacheKey = req.url;//url.parse(req.url).pathname;
			var body = buffers();

			if (req.method.toUpperCase() == "GET") {
				cacheLoad(cacheKey, function(err, entry, initial) {
					if (err || !entry || !entry.body || !entry.body.length) {
						render();
					} else {
						if (initial && entry.dependencies) {
							monitor(entry.dependencies);
						}

						for (var name in entry.headers) {
							res.header(name, entry.headers[name]);
						}

						sendFromCache(entry);				
					}
				});
			} else {
				render();
			}
		} catch (exc) {
			sendError(exc);
		}

		function render() {
			var write = res.write;
		    res.write = function(data, encoding) {
		    	body.push(new Buffer(data, encoding));
		    }

	    	var end = res.end;
		    res.end = function(data, encoding) {
		    	body.push(new Buffer(data, encoding));

				res.write = write;
				res.end = end;

				// XXXjoe It's unfortunate we have to make a copy here, but passing the Buffers
				// object directly through the system doesn't work... investigate!
				body = body.slice();

				sendFromApp(cacheKey, body);
		    }

		    // XXXjoe Make sure to unlock the cache if there is an error
			cache.lock(cacheKey);
			next();			
		}

		function cacheLoad(cacheKey, cb) {
			if (cache) {
				cache.load(cacheKey, cb);	
			} else {
				cb(0, null);
			}
		}

		function cacheStore(cacheKey, entry, cb) {
			if (cache) {
				cache.store(cacheKey, entry, cb);
			} else {
				cb(0, entry);
			}
		}
		
		function monitor(dependencies) {
			if (cache && dependencies) {
				cache.monitor(cacheKey, dependencies);
			}
		}

		function sendFromApp(cacheKey, body) {
			if (res.statusCode == 200 && !res.doNotCache) {
				var headers = {};
				exports.headersToCache.forEach(function(name) {
					headers[name] = res.header(name);
				});

				var entry = {key: cacheKey, headers: headers, body: body};
				if (res.dependencies) {
					entry.dependencies = res.dependencies;
					monitor(res.dependencies);	
				}

				var contentType = res.header('content-type');
				var m = reContentType.exec(contentType);
				entry.mimeType = m ? m[1] : contentType;
				entry.charset = m ? m[2] : null;

				cacheStore(cacheKey, entry, function(err, entry) {
					sendFromCache(entry);
				});
			} else {
				cache.unlock(cacheKey);
				res.send(body);
			}
		}

		function sendFromCache(entry) {
			var etag = res.header('etag');
			var ifNoneMatch = req.header('if-none-match');
			if (ifNoneMatch && ifNoneMatch == etag) {
				res.send('', 304);
			} else {
				var body = entry.body;
				if (entry.bodyZipped && canZipType(entry.mimeType) && requestAccepts(req, 'gzip')) {
					body = entry.bodyZipped;
					res.header('Content-Encoding', 'gzip');
				}

				res.header('Content-Length', body.length);
				res.send(body, 200);
			}
		}

		function sendError(err) {
			dandy.logException(err);
			res.send('Error: ' + err, {'Content-Type': 'text/html'}, 500);			
		}
	}	
}

function canZipType(mimeType) {
	return mimeType.indexOf('image/') != 0 
		   && exports.gzipBlacklist.indexOf(mimeType) == -1;
}

function requestAccepts(req, encoding) {
	var accepts	= 'accept-encoding' in req.headers ? req.header('accept-encoding').split(/\s*,\s*/) : [];
	return accepts.indexOf(encoding) != -1;
}
