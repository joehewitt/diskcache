
var dandy = require('dandy/errors');
var _ = require('underscore');
var buffers = require('buffers');

// Whitelist of headers to cache - should it be a blacklist instead?
exports.headersToCache = [
	"content-type",
	"etag",
	"cache-control"
];

// *************************************************************************************************

exports.middleware = function(cache, category) {
	return function(req, res, next) {
		try {
			var cacheKey = req.url;//url.parse(req.url).pathname;
			var body = buffers();

			cacheLoad(cacheKey, function(err, entry, initial) {
				if (err || !entry || !entry.body || !entry.body.length) {
	    			var write = res.write;
				    res.write = function(data, encoding) {
				    	body.push(new Buffer(data, encoding));
				    }

			    	var end = res.end;
				    res.end = function(data, encoding) {
				    	body.push(new Buffer(data, encoding));

						res.write = write;
						res.end = end;

						sendFromApp(cacheKey, body.slice()+'');
				    }

				    // XXXjoe Make sure to unlock the cache if there is an error
					cache.lock(cacheKey);
					next();
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
		} catch (exc) {
			sendError(exc);
		}

		function cacheLoad(cacheKey, cb) {
			if (cache) {
				cache.load(cacheKey, category, cb);	
			} else {
				cb(0, null);
			}
		}

		function cacheStore(cacheKey, entry, cb) {
			if (cache) {
				cache.store(cacheKey, entry, category, cb);
			} else {
				cb(0, entry);
			}
		}

		function monitor(dependencies) {
			if (cache && dependencies) {
				var filePaths = _.pluck(dependencies, 'path');
				cache.monitor(cacheKey, null, filePaths);
			}
		}

		function sendFromApp(cacheKey, body) {
			if (res.statusCode == 200 && !res.doNotCache) {
				var headers = {};
				exports.headersToCache.forEach(function(name) {
					headers[name] = res.header(name);
				});

				var entry = {headers: headers, body: body};
				if (res.dependencies) {
					entry.dependencies = res.dependencies;
					monitor(res.dependencies);	
				}

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
			var ifNoneMatch = req.headers['if-none-match'];
			if (ifNoneMatch && ifNoneMatch == etag) {
				res.send('', 304);
			} else {
				var body = entry.body;
				if (entry.bodyZipped && requestAccepts(req, 'gzip')) {
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

function requestAccepts(req, encoding) {
	var accepts	= 'accept-encoding' in req.headers ? req.headers['accept-encoding'].split(/\s*,\s*/) : [];
	return accepts.indexOf(encoding) != -1;
}
