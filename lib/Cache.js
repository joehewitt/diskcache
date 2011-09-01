
var D;

var crypto = require('crypto');
var path = require('path');
var events = require('events');
var fs = require('fs');
var url = require('url');
var _ = require('underscore');
var mkdirsSync = require('mkdir').mkdirsSync;
var rimraf = require('rimraf');
var gzip = require('gzip');

// *************************************************************************************************

/**
 * Disk cache manages an cache on disk and optionally in memory.
 *
 * Any object which can be converted to JSON can be stored.
 */
function Cache(cachePath, useDisk, useMem, useGzip) {
    events.EventEmitter.call(this);

	this.cachePath = cachePath;
	this.useMem = useMem;
	this.useDisk = useDisk;
	this.useGzip = useGzip;
	this.memCache = {};
	this.memCacheDirs = {};
	this.locks = {};
	this.monitors = {};
	if (cachePath) {
		mkdirsSync(cachePath);
	}
}
exports.Cache = Cache;

function subclass(cls, supercls, proto) {
    cls.super_ = supercls;
    cls.prototype = Object.create(supercls.prototype, {
        constructor: {value: cls, enumerable: false}
    });
    _.extend(cls.prototype, proto);
}

subclass(Cache, events.EventEmitter, {
	/**
	 * Loads the entry for a URL from the cache.
	 *
	 * If useGzip is true and cached object has a body property, the result
	 * will have a bodyZipped property that is a zipped buffer.
	 */
	load: function(URL, cb) {
		var locks = this.locks[URL];
		if (locks) {
			D&&D('Wait for lock on', URL);
			locks.push(cb);
		} else {
			if (URL in this.memCache) {
				cb(0, this.memCache[URL]);
			} else if (this.useDisk) {
				var keys = this._keysForURL(URL);
				D&&D('Try to load', keys.path, 'for', URL);
				fs.readFile(keys.path, _.bind(function(err, jsonData) {
					if (err) return cb ? cb(err) : 0;

					var data = JSON.parse(jsonData);
					this._storeAndZip(URL, keys, data, cb);
				}, this));				
			} else {
				cb(new Error("Not found in cache"));
			}
		}
	},
	
	/**
	 * Stores data in cache.
	 */	
	store: function(URL, data, cb) {
		// It is important to stringify the data here, because gzip may
		// modify it further down, and we don't want to cache the gzipped body
		var jsonData = JSON.stringify(data);

		var keys = this._keysForURL(URL);
		this._storeAndZip(URL, keys, data, _.bind(phase2, this));

		function phase2() {
			if (this.useDisk) {
				fs.writeFile(keys.path, jsonData, 'utf8', _.bind(function(err) {
					phase3.apply(this, [err]);
				}, this));
			} else {
				phase3.apply(this);
			}
		}

		function phase3(err) {
			if (cb) {
				cb(err, data);
			}
			this.unlock(URL, data);
		}
	},

	/**
	 * Removes cached data for a url.
	 */
	remove: function(URL, cb) {
		var keys = this._keysForURL(URL);

		if (this.useDisk) {
			D&&D('Remove', keys.path, 'for', URL);			
			fs.unlink(keys.path, cb);
		}
		
		if (this.useMem) {
			delete this.memCache[URL];
			// XXXjoe Remove from keys.mem
		}
	},
	
	/**
	 * Removes all files in the cache for a directory.
	 */
	removeAll: function(URL, cb) {
		if (URL) {
			var keys = this._keysForURL(URL, true);
			try {
				if (this.useDisk) {
					rimraf(keys.path, function(err) {
						if (cb) cb(err);
					});
				}

				if (this.useMem) {
					for (var branchURL in keys.mem) {
						for (var leafURL in keys.mem[branchURL]) {
							delete this.memCache[leafURL];
						}
						delete this.memCache[branchURL];
						delete keys.mem[branchURL];
					}
				}
			} catch (exc) {
				console.log(exc);
			}
		} else {
			if (this.useDisk && this.cachePath) {
				rimraf(this.cachePath, {gently: true}, function(err) {
					if (cb) cb(err);
				});
			}

			if (this.useMem) {
				this.memCache = {};
				this.memCacheDirs = {};
			}
		}
	},

	/**
	 * Locks a URL so that it can't be accessed until it is stored.
	 */	
	lock: function(URL) {
		D&&D('Lock', URL);
		this.locks[URL] = [];
	},

	/**
	 * Unlocks a URL and dispatches its new value to callbacks waiting on the lock.
	 */	
	unlock: function(URL, data) {
		D&&D('Unlock', URL);
		var callbacks = this.locks[URL];
		if (callbacks) {
			delete this.locks[URL];

			callbacks.forEach(function(cb) {
				cb(0, data);
			});
		}
	},

	/**
	 * Invalidates a given URL when changes affect dependent files.
	 *
	 * @dependencies An array of file paths to start monitoring.
	 */
	monitor: function(URL, dependencies, cb) {
		var mon = _.bind(function() {
			_.each(dependencies, _.bind(function(depPath) {
				var monitors = this.monitors[depPath];
				var index = monitors.indexOf(mon);
				monitors.splice(index, 1);
				if (!monitors.length) {
					D&&D('Unwatch', depPath, URL);
					fs.unwatchFile(depPath);
					delete this.monitors[depPath];
				}
			}, this));

			this.remove(URL);
			if (cb) { cb(0, URL); }				
			this.emit('unmonitor', {url: URL});
		}, this);

		_.each(dependencies, _.bind(function(depPath) {
			D&&D('Watch', depPath, URL);
			if (depPath in this.monitors) {
				this.monitors[depPath].push(mon);
			} else {
				this.monitors[depPath] = [mon];	

				fs.watchFile(depPath, _.bind(function(curr, prev) {
					if (curr.mtime.getTime() != prev.mtime.getTime()) {
						D&&D("Modified", depPath, curr.mtime);
						_.each(this.monitors[depPath], function(fn) { fn(); });
					}
				}, this));					
			}
		}, this));
	},

	/**
	 * Stops monitoring changes to files for a given url.
	 *
	 * @dependencies An array of file paths to stop monitoring.
	 */
	unmonitor: function(URL, dependencies) {
		_.each(dependencies, function(depPath) {
			fs.unwatchFile(depPath);
		});
		this.emit('unmonitor', {url: URL});
	},

	_keysForURL: function(URL, justDirectory) {
		var keys = {};
		var hash = this._hashForURL(URL);

		var U = url.parse(URL);
		var cachePath = path.join(this.cachePath, U.pathname);

		if (this.useDisk) {
			mkdirsSync(cachePath);
		}

		if (this.useMem) {
			var parts = U.pathname ? U.pathname.split('/') : [];
			var dir = this.memCacheDirs;
			_.each(parts, function(part) {
				if (part in dir) {
					dir = dir[part];
				} else {
					dir = dir[part] = {};
				}
			});
			keys.mem = dir;
		}

		if (justDirectory) {
			keys.path = cachePath;
		} else {
			var fileName = hash + '.txt';
			keys.path = path.join(cachePath, fileName);
		}

		return keys;
	},

	_hashForURL: function(URL) {
		var hash = crypto.createHash('md5');
		hash.update(URL);
		return hash.digest('hex');
	},

	_storeInMemCache: function(URL, keys, data) {
		this.memCache[URL] = data;
		keys.mem[URL] = 1;
	},

	_storeAndZip: function(URL, keys, data, cb) {
		if (this.useMem) {
			if (this.useGzip && data.body) {
				var body = data.body;
				if (body && body.toString) {
					body = body.toString('utf8');
				}
		    	gzip(body, _.bind(function(err, gzipped) {
		    		data.bodyZipped = gzipped;
		    		this._storeInMemCache(URL, keys, data);
					cb(0, data, true);
		    	}, this));
		    } else {
				this._storeInMemCache(URL, keys, data);
				cb(0, data, true);
		    }
		} else {
			cb(0, data, true);
		}		
	}	
});

exports.Cache = Cache;
